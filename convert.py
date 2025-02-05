from typing import Dict, List, Tuple, Mapping
import random
import os
import json
import logging
import argparse
import requests
import re
from urllib.parse import urlparse, parse_qs
from datetime import datetime, timezone
from collections import defaultdict
from typing import Dict, List, Tuple
from rich.console import Console
from rich.progress import track
from rich.logging import RichHandler
import networkx as nx
import aiohttp
import asyncio
import yaml

class ModelAPI:
    def __init__(self, enabled=True, max_reqs=20):
        self.api_key = os.getenv("MODEL_API_KEY", "") if enabled else ""
        self.api_endpoint = os.getenv("MODEL_API_ENDPOINT", "") if enabled else ""
        self.model_name = os.getenv("MODEL_NAME", "") if enabled else ""

        self.headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.api_key}"
        }

        self.available = all([self.api_key, self.api_endpoint, self.model_name])
        self.semaphore = asyncio.Semaphore(max_reqs)

    async def send_message(self, session:aiohttp.ClientSession, message):
        if not self.available:
            return None

        payload = {
            "model": self.model_name,
            "messages": [
                {"role": "user", "content": message}
            ]
        }

        async with self.semaphore: # limit max concurrency
            async with session.post(self.api_endpoint, headers=self.headers, json=payload) as response:
                return await response.json() if response.status == 200 else {"error": await response.text()}


# Configure logging
def setup_logging() -> logging.Logger:
    console = Console()
    logging.basicConfig(
        level=logging.INFO,
        format="%(message)s",
        datefmt="[%X]",
        handlers=[RichHandler(console=console, rich_tracebacks=True)]
    )
    return logging.getLogger("rich")

log = setup_logging()

def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Process tweets and generate markdown files.")
    parser.add_argument("--input", required=True, help="Path to the input JSON file")
    parser.add_argument("--output", required=True, help="Path to the output directory")
    parser.add_argument("--user_id", required=True, help="User ID to identify threads")
    parser.add_argument("--user_name", required=True, help="User name to link to twitter")
    parser.add_argument("--draft_before_date", required=False, default="2018-01-01", help="All tweets before this isoformat date will be put in draft mode to avoid publishing old tweets")
    return parser.parse_args()

def convert_to_utc(dt_str: str) -> datetime:
    # twitter date format is "Tue Feb 04 18:34:08 +0000 2025"
    return datetime.strptime(dt_str, "%a %b %d %H:%M:%S +0000 %Y").astimezone(timezone.utc)

def find_thread_root(tweet_id: str, reply_graph: nx.DiGraph) -> str:
    while True:
        predecessors = list(reply_graph.predecessors(tweet_id))
        if not predecessors:
            return tweet_id
        tweet_id = predecessors[0]

def get_thread_sequence(root_id: str, tweet_map: Dict[str, Dict], reply_graph: nx.DiGraph) -> List[str]:
    tweets = [(t, convert_to_utc(tweet_map[t]["created_at"])) for t in nx.dfs_preorder_nodes(reply_graph, source=root_id)]
    return [t[0] for t in sorted(tweets, key=lambda x: x[1])]

def parse_triple_dot_endings(text: str):
    """
    Looks at the input multiline string and checks if it ends with the following pattern:

        <prefix><three dots><whitespace><zero or more URLs separated by whitespace>

    Returns a tuple:
       (is_pattern_found, prefix, list_of_urls)

    - is_pattern_found: True if the pattern is found at the end of the string, False otherwise.
    - prefix: The part of the string before the three dots.
    - list_of_urls: A list of URL tokens (if any) that follow the three dots.
    """
    # The regex explanation:
    #   ^                         : start of string
    #   (?P<prefix>.*)            : capture any characters (including newlines) as prefix (greedy)
    #   \.\.\.                    : literally three dots
    #   \s+                       : at least one whitespace character (required)
    #   (?P<urls>(?:\S+\s*)*)     : capture zero or more groups of non-whitespace (the URL)
    #                              characters optionally followed by whitespace.
    #   $                         : end of string
    #
    # We use re.DOTALL so that '.' matches newline characters as well.
    pattern = re.compile(
        r'^(?P<prefix>.*?)\.\.\.\s+(?P<urls>(?:https?://\S+(?:\s+|$))*)$',
        re.DOTALL
    )
    match = pattern.fullmatch(text)
    if not match:
        return False, None, []

    prefix = match.group('prefix')
    urls_str = match.group('urls').strip()  # Remove any trailing spaces
    # If there are URLs, split on whitespace, otherwise return an empty list.
    urls = urls_str.split() if urls_str else []

    return True, prefix, urls


def classify_tweets(tweet_map: Dict[str, Dict], reply_graph:nx.DiGraph) -> None:
    # first identify replies and retweets
    for tweet in tweet_map.values():
        text = tweet["full_text"].strip()
        assert "type" not in tweet

        # We adopt pretty bad heuristics here because apparently data don't have critical info
        # 1. below seems to be the only way to identify retweets
        # 2. also, quoted tweets won't have RT prefix! There is no reliable way to identify quoted tweets
        # 3. retweets also don't have in_reply_to_status_id_str
        if text.startswith("RT @"):
            tweet["type"] = "Retweet"
        elif "in_reply_to_status_id_str" in tweet:
            # note that self replies won't start with "@<reply-to-user>"
            tweet["type"] = "Reply"
        else:
            tweet["type"] = "Post" # this will include quoted tweets

    # Now identify threads. A chain of reply posts are not considered as a thread
    for tweet_id, tweet in tweet_map.items():
        assert "type" in tweet, f"at this point all tweets should have type: {tweet}"
        if tweet.get("is_thread", ""): # only process candidate threads
            # is it a root tweet?
            root_id = find_thread_root(tweet_id, reply_graph)
            if root_id != tweet_id:
                continue
            if tweet["type"] != "Post":
                continue
            # get the chain and if start node is a post then it's a thread
            sequence = get_thread_sequence(root_id, tweet_map, reply_graph)
            assert all(tweet_map[t]["is_thread"] for t in sequence), f"Thread has non-thread tweets: {sequence}"
            assert len(sequence) > 1, f"Thread has only one tweet: {sequence}"
            # if the the first tweet is a post then we have a thread
            for t in sequence:
                tweet_map[t]["type"] = "Thread"

def generate_storage_name(tweet: Dict) -> str:
    dt_utc = convert_to_utc(tweet["created_at"])
    return f"{dt_utc.strftime('%Y%m%d%H%M')}"

def build_graph(tweet_map: Dict[str, Dict], reply_graph: nx.DiGraph, user_id:str) -> None:
    for tweet_id, tweet in tweet_map.items():
        if not reply_graph.has_node(tweet_id):
            reply_graph.add_node(tweet_id, data=tweet)
        from_tweet_id = tweet.get("in_reply_to_status_id_str", "")
        if from_tweet_id and from_tweet_id in tweet_map:
            if not reply_graph.has_node(from_tweet_id):
                reply_graph.add_node(from_tweet_id, data=tweet_map[from_tweet_id])
            reply_graph.add_edge(from_tweet_id, tweet_id)
            reply_to_user_id = tweet.get("in_reply_to_user_id")
            if reply_to_user_id == user_id:
                tweet["is_thread"] = True
                tweet_map[from_tweet_id]["is_thread"] = True

def post_link(tweet: Dict, args):
    if 'id_str' in tweet and args.user_name:
        return f"https://x.com/{args.user_name}/status/{tweet['id_str']}"



def extract_twitter_urls(text):
    # This regular expression will find all occurrences of http://t.co/{id}
    pattern = r"https?://t\.co/[\w\d]+"

    # Find all non-overlapping matches in the text.
    matches = re.findall(pattern, text)
    return matches


def extract_tweet_info(url):
    # Regex breakdown:
    #   ^https://(x|twitter)\.com/       : URL must be x.com or twitter.com
    #   ([^/]+)               : Capture group for {user} (one or more characters not '/')
    #   /status/              : Literal string "/status/"
    #   ([^/?]+)              : Capture group for {id} (one or more characters not '/' or '?')
    #   (?:\?.*)?             : Optionally, a '?' followed by any characters (query parameters)
    #   $                     : End of string
    pattern = r"^https?://(x|twitter)\.com/([^/]+)/status/([^/?]+)(?:\?.*)?$"

    match = re.match(pattern, url)
    if match:
        user = match.group(2)
        tweet_id = match.group(3)
        assert user and tweet_id, f"User and tweet ID not found in URL: {url}"
        return (tweet_id, user)
    return None


def build_url_map(tweet_map: Dict[str, Dict]):
    for tweet in tweet_map.values():
        url_map = {}
        content = tweet["full_text"]
        if "entities" in tweet and "urls" in tweet["entities"]:
            for url_dict in tweet["entities"]["urls"]:
                urls = extract_twitter_urls(url_dict["url"])
                assert len(urls) == 1, f"Expected 1 URL, found {len(urls)} in {url_dict['url']}"
                expanded = url_dict["expanded_url"]
                # let's not replace every twitter link with shortcode for now because quoted tweets have it end  and shouldn't be replaced
                # tweet_info = extract_tweet_info(expanded)
                # if tweet_info:
                #     tweet_id, user = tweet_info
                #     url_map[urls[0]] = tweet_shortcode(tweet_id, user)
                # else:
                url_map[urls[0]] = expanded
        tweet["url_map"] = url_map

def build_media_map(tweet_map: Dict[str, Dict]):
    for tweet in tweet_map.values():
        media_map = {}
        content = tweet["full_text"]
        if "entities" in tweet and "media" in tweet["entities"]:
            for media_dict in tweet["entities"]["media"]:
                urls = extract_twitter_urls(media_dict["url"])
                assert len(urls) == 1, f"Expected 1 URL, found {len(urls)} in {media_dict['url']}"
                expanded = media_dict["media_url_https"]
                media_map[urls[0]] = expanded
        tweet["media_map"] = media_map

async def download_image(session, url, folder, filename):
    os.makedirs(folder, exist_ok=True)
    file_path = os.path.join(folder, filename)
    try:
        async with session.get(url) as response:
            if response.status == 404:
                return None
            response.raise_for_status()
            with open(file_path, "wb") as file:
                file.write(await response.read())
        return file_path
    except Exception as e:
        return e


def id_from_url(url):
    pattern = r"https?://[^/]+/([^/]+)"
    match = re.search(pattern, url)
    return match.group(1) if match else None

def get_redirected_url(url, timeout=10):
    try:
        # Using a HEAD request first can be more efficient,
        # but some servers don't handle HEAD properly so we fall back to GET.
        response = requests.head(url, allow_redirects=True, timeout=timeout)
        if response.status_code >= 400:
            # If the HEAD request fails, fall back to a GET request.
            response = requests.get(url, allow_redirects=True, timeout=timeout)
        # response.url is the final URL after redirection
        return response.url
    except requests.RequestException as e:
        # If there's an error, you might want to log or handle it appropriately.
        # For now, we'll just return the original URL.
        return url

def build_twittr_url_replacements(tweet_map: Dict[str, Dict]) -> None:
    for tweet in tweet_map.values():
        urls = extract_twitter_urls(tweet["full_text"])
        replacements = {}

        for url in urls:
            if url in tweet["url_map"]:
                replacements[url] = { 'expanded': tweet["url_map"][url] }
            elif url in tweet["media_map"]:
                expanded = tweet["media_map"][url]
                ext = os.path.splitext(urlparse(expanded).path)[-1]
                id = id_from_url(url)
                assert id, f"id not found in url {url}"
                replacements[url] = {
                    'expanded': expanded,
                    'media_filename': f'{id}{ext}',
                    'image_alt': ''
                }
            else:
                final_url = get_redirected_url(url)
                replacements[url] = { 'expanded': final_url }

        tweet["replacements"] = replacements

def sanitize_filename(filename: str) -> str:
    # Define a regex pattern that matches any illegal character.
    # Note: The backslash '\' must be escaped.
    illegal_chars_pattern = r'[<>:"/\\|?*]'
    sanitized = re.sub(illegal_chars_pattern, "", filename)

    # Optionally, you can also strip leading/trailing whitespace.
    return sanitized.strip()

def merge_replacements(dict1, dict2):
    merged = {}

    # Combine the keys from both dictionaries.
    all_keys = set(dict1.keys()).union(dict2.keys())

    for key in all_keys:
        if key in dict1 and key in dict2:
            cdict1, cdict2, cmerged = dict1[key], dict2[key], {}
            for ckey in set(cdict1.keys()).union(cdict2.keys()):
                if ckey in cdict1 and ckey in cdict2:
                    if cdict1[ckey] != cdict2[ckey]:
                        raise ValueError(f"Conflict for key '{key}': {cdict1[ckey]} != {cdict2[ckey]}")
                    cmerged[ckey] = cdict1[ckey]  # or cdict2[ckey] (they are the same)
            merged[key] = dict1[key]  # or dict2[key] (they are the same)
        elif key in dict1:
            merged[key] = dict1[key]
        else:  # key is only in dict2
            merged[key] = dict2[key]

    return merged

def sanitize_yaml_line(value):
    # Dump using PyYAML's safe representation
    safe_value = yaml.dump(value, allow_unicode=True, default_style=None, width=float('inf'))

    # dump function is pretty bad and randomly adds "\n...\n", quotes, front separator etc

    safe_value = safe_value.lstrip("'")
    safe_value = safe_value.strip()
    safe_value = safe_value.rstrip("'")
    safe_value = safe_value.strip()
    safe_value = safe_value.lstrip("-")
    safe_value = safe_value.strip()
    safe_value = safe_value.rstrip(".")
    safe_value = safe_value.strip()

    return safe_value

def is_draft(tweet, draft_before_date:str):
    # if its not a post or thread then its a draft
    if tweet["type"] not in ["Post", "Thread"]:
        return True

    if draft_before_date:
        # if tweet is before draft_before_date then it's a draft (assume isoformat strings)
        last_date = datetime.fromisoformat(draft_before_date)
        # if doesn't have time zone then force UTC
        if last_date.tzinfo is None:
            last_date = last_date.astimezone(timezone.utc)
        if convert_to_utc(tweet["created_at"]) < last_date:
            return True
    return False

async def build_frontmatter(session, api: ModelAPI, tweet, args):
    """
    Build the markdown frontmatter for a tweet blog post.

    Parameters:
        session: The HTTP session to be used for API requests.
        api (ModelAPI): An instance of ModelAPI to generate title and slug.
        tweet (dict): The tweet data.
        draft (bool): Whether the post is a draft.

    Returns:
        A tuple (frontmatter, slug) where frontmatter is the markdown string,
        and slug is the generated slug.
    """
    # Convert tweet creation time to UTC and generate a formatted date string.
    frontmatter, slug = None, None

    if api.available:
        retries = 5
        while retries > 0:
            if retries < 5:
                log.warning(
                    f"Retry {5 - retries} to get frontmatter for tweet: {tweet.get('id_str', '<NA>')}"
                )
                # Sleep for a random interval between 2 and 30 seconds.
                await asyncio.sleep(random.uniform(2, 30.0))
            retries -= 1

            try:
                prompt = (
                    "For below tweet, create a very short creatively funny but clever and informative title "
                    "for the frontmatter to be used in blog and return it in the first line.\n"
                    "In the next line, create a short valid file name where this blog post can be saved.\n"
                    "Do not include anything else in your response.\n\n"
                    f"{tweet['full_text']}"
                )
                response = await api.send_message(session, prompt)
            except Exception as e:
                log.warning(f"Model API request failed: {e}")
                await asyncio.sleep(random.uniform(2, 30.0))
                continue

            # Process the API response if it is a mapping.
            if not isinstance(response, Mapping):
                continue

            choices = response.get("choices")
            if not choices or not isinstance(choices, list):
                continue

            message = choices[0].get("message")
            if not message or "content" not in message:
                continue

            # Split the message content into non-empty lines.
            lines = [line.strip() for line in message["content"].split("\n") if line.strip()]
            if len(lines) < 2:
                continue

            # Process title.
            title = lines[0].replace('"', "'").strip()
            if len(title) < 3:
                continue
            if title.startswith("'") and title.endswith("'"):
                title = title[1:-1].strip()
            if len(title) < 3:
                continue
            title = sanitize_yaml_line(title)

            # Process slug.
            raw_slug = lines[1].strip()
            slug = sanitize_filename(raw_slug)
            if slug.endswith(".md"):
                slug = slug[:-3]
            slug = generate_storage_name(tweet) + '-' + slug
            slug = sanitize_yaml_line(slug)

            draft = is_draft(tweet, args.draft_before_date)

            # Build the frontmatter markdown string.
            frontmatter = (
                f"---\n"
                f"title: '{title}'\n"
                f"draft: {str(draft).lower()}\n"
                # https://gohugo.io/content-management/front-matter/#dates
                f"date: {convert_to_utc(tweet['created_at']).isoformat()}\n" # frontmatter accepts isoforamt strings like '2025-02-05T02:34:08+00:00'
                f'slug: "{slug}"\n'
                f'is_tweet: true\n'
                f'tweet_info:\n'
                f'  id: "{tweet["id_str"]}"\n'
                f'  type: "{tweet["type"].lower()}"\n'
                f'  is_thread: {tweet.get("is_thread", False)}\n'
                f"---\n\n"
            )
            break  # Success; exit the retry loop.

    return frontmatter, slug


def twitter_handles_to_links(text):
    # Using (?<!\S) ensures that the character before '@' is not a non-whitespace character,
    # i.e. it's either the start of the string or a whitespace.
    pattern = r"(?<!\S)@(\w+)"

    def repl(match):
        handle = match.group(1)
        # Replace with markdown formatted link: [handle](https://x.com/handle)
        return f"[@{handle}](https://x.com/{handle})"

    return re.sub(pattern, repl, text)

def tweet_shortcode(tweet_id, user_name):
    params = f'user="{user_name}" id="{tweet_id}"'
    shortcode = '{{< tweet ' + params + ' >}}'
    return shortcode

def youtube_to_shortcode(text: str) -> str:
    # This pattern matches any substring that starts with < and ends with >
    pattern = r'<([^>]+)>'

    def replace(match):
        url = match.group(1)
        parsed = urlparse(url)
        video_id = None
        host = parsed.netloc.lower()

        # Check for standard YouTube URLs like "youtube.com/watch?v=VIDEO_ID"
        if 'youtube.com' in host:
            if parsed.path == '/watch':
                qs = parse_qs(parsed.query)
                if 'v' in qs:
                    video_id = qs['v'][0]
            # Also support URLs like "youtube.com/embed/VIDEO_ID"
            elif parsed.path.startswith('/embed/'):
                video_id = parsed.path.split('/embed/')[1]

        # Check for short YouTube URLs like "youtu.be/VIDEO_ID"
        elif 'youtu.be' in host:
            # The video id is the first element of the path (after removing any leading '/')
            video_id = parsed.path.lstrip('/')

        # If we found a video id, return the shortcode; otherwise return the original match.
        if video_id:
            return "{{< youtube {" + video_id + "} >}}"
        else:
            return match.group(0)

    return re.sub(pattern, replace, text)

async def convert_tweet(session, tweet_id:str, tweet: Dict, reply_graph:nx.DiGraph, tweet_map:Dict[str, Dict], api:ModelAPI, args:argparse.Namespace):
    tweet_type = tweet["type"]
    # new line after frontmatter
    tweet["mark_down"] = tweet['full_text']

    mal_formed, download_failed, api_failed = 0, 0, 0
    if tweet_type == "Thread": # club content of a thread
        # combine markdowns for all tweets in the thread
        root_id = find_thread_root(tweet_id, reply_graph)
        if root_id != tweet_id:
            return mal_formed, download_failed, api_failed
        sequence = get_thread_sequence(root_id, tweet_map, reply_graph)
        merged_replacements = {}
        for t in sequence:
            merged_replacements = merge_replacements(merged_replacements, tweet_map[t]["replacements"])
        # For markdown we need two newlines between each tweet
        thread_text = "\n\n".join([tweet_map[t]["mark_down"].strip() for t in sequence])

        tweet = tweet_map[root_id]
        tweet["mark_down"] = thread_text
        tweet["replacements"] = merged_replacements
    elif tweet_type == "Reply": # replace @ with tweet link
        # do we have info for "To" tweet?
        if 'in_reply_to_screen_name' in tweet and "in_reply_to_status_id_str" in tweet:
            reply_to_id = tweet["in_reply_to_status_id_str"]
            reply_to_user_id = tweet["in_reply_to_screen_name"]
            # get first word which should be the handle
            parts = tweet['mark_down'].strip().split(maxsplit=1)
            assert len(parts) == 2, f"First word and then rest of the test expcted: {tweet['mark_down']}"
            assert parts[0].startswith("@"), f"First word should be a handle: {parts[0]}"

            # "To" tweet using shortcode
            response_text = tweet_shortcode(reply_to_id, reply_to_user_id) + '\n\n' + parts[1]
            tweet["mark_down"] = response_text
        else:
            mal_formed += 1
        # else sometimes this info is missing and we can't do anything
    # else no other processing for other types

    # For retweets and quoted tweets, twitter truncates and ends with ... followed by URLs, first being the original tweet and rest being media
    is_pattern_found, prefix, urls = parse_triple_dot_endings(tweet["mark_down"])
    if is_pattern_found and prefix:
        tweet["mark_down"] = prefix + '...' + ' [continue reading](urls[0])'
        for url in urls[1:]:
            tweet["mark_down"] += f"\n\n{url}"

    # generate default storage name, i.e., filename or folder name if tweet has media
    storage_name = generate_storage_name(tweet)

    # get frontmatter and slug (slug will replace storage_name)
    frontmatter, slug = await build_frontmatter(session, api, tweet, args)
    if frontmatter and slug:
        storage_name = slug
    else:
        api_failed += 1
        create_date = convert_to_utc(tweet['created_at'])
        slug = storage_name
        frontmatter = (
            f"---\n"
            f'title: "{create_date.isoformat()}"\n'
            f"draft: true\n"
            f"date: {create_date.isoformat()}\n" # frontmatter accepts isoforamt strings like '2025-02-05T02:34:08+00:00'
            f'slug: "{slug}"\n'
            f"---\n\n"
        )
    tweet["mark_down"] = frontmatter + '\n\n' + tweet["mark_down"]

    # now we will replace the followings:
    # 1. twitter handles with markdown links
    # 2. media URLs with local file paths
    # 3. other URLs with markdown links
    # first assert that storage name doesn't have any bad characters including . or new line
    assert not re.search(r'[<>:"/\\|?*\.\n]', storage_name), f"Storage name has bad characters: {storage_name}"
    base_folder = os.path.join(args.output, tweet["type"].lower())
    os.makedirs(base_folder, exist_ok=True)
    # check if _index.md exists, if not then create default index.md
    if not os.path.exists(os.path.join(base_folder, "_index.md")):
        with open(os.path.join(base_folder, "_index.md"), "w", encoding="utf-8") as f:
            f.write(f"---\ntitle: Twitter {tweet['type']}\n---\n\n")

    # assume default as md file but we will change it to folder if tweet has media
    content_filepath = os.path.join(base_folder, storage_name + ".md")
    if tweet["replacements"]:
        for url, replacement in tweet["replacements"].items():
            expanded_url = replacement['expanded']
            if replacement.get("media_filename"): # creat sub-folder for media files
                content_folder = os.path.join(base_folder, storage_name)
                os.makedirs(content_folder, exist_ok=True)
                content_filepath = os.path.join(content_folder, "index.md")

                filepath = await download_image(session, expanded_url, content_folder, replacement["media_filename"])
                if filepath: # download success
                    tweet["mark_down"] = tweet['mark_down'].replace(url,
                        f"\n\n![{replacement['image_alt'] or expanded_url}]({replacement['media_filename']})")
                else: # download failed
                    download_failed += 1
                    tweet["mark_down"] = tweet["mark_down"].replace(url,
                        f"\n\n![{replacement['image_alt'] or url}]({expanded_url})")
            else: # not a media file URL
                tweet["mark_down"] = tweet["mark_down"].replace(url, f'<{expanded_url}>') # put URL in <> for markdown
                # if URL was already in markdown, i.e., inside [...]() then remove <> that we just added
                tweet["mark_down"] = tweet["mark_down"].replace(f'](<{expanded_url}>)', f']({expanded_url})')
                # also fix shortcodes
                tweet["mark_down"] = tweet["mark_down"].replace(f'<{{< {expanded_url} >}}>', f'{{< {expanded_url} >}}')
                tweet["mark_down"] = youtube_to_shortcode(tweet["mark_down"])
    # turn twitter handles into markdown links
    tweet["mark_down"] = twitter_handles_to_links(tweet["mark_down"]).strip()

    # add post link
    if tweet["mark_down"].endswith("...") or tweet["mark_down"].endswith("…"):
        tweet["mark_down"] += f" [continue reading]({post_link(tweet, args)})"
    else:
        tweet["mark_down"] += f"\n\n[Discussion]({post_link(tweet, args)})"

    # markdown doesn't end with a newline, add it
    tweet["mark_down"] = tweet["mark_down"].strip() + '\n'

    # save the markdown content
    os.makedirs(os.path.dirname(content_filepath), exist_ok=True)
    with open(content_filepath, "w", encoding="utf-8") as f:
        f.write(tweet["mark_down"])
    log.info(f"Saved: {content_filepath}")

    return mal_formed, download_failed, api_failed

async def main() -> None:
    import time
    start_time = time.perf_counter()
    args = parse_arguments()
    mal_formed, download_failed, api_failed = 0, 0, 0

    api = ModelAPI(enabled=True)

    os.makedirs(args.output, exist_ok=True)
    with open(args.input, "r", encoding="utf-8") as f:
        tweets_data = json.load(f)

    tweet_map: Dict[str, Dict] = {}
    reply_graph = nx.DiGraph()
    for item in track(tweets_data, description="Building tweet map..."):
        tweet = item["tweet"]
        tweet_id = tweet["id_str"]
        assert tweet_id not in tweet_map
        tweet_map[tweet_id] = tweet

    assert len(tweet_map) == len(tweets_data)

    build_graph(tweet_map, reply_graph, args.user_id)
    classify_tweets(tweet_map, reply_graph)
    build_url_map(tweet_map)
    build_media_map(tweet_map)
    build_twittr_url_replacements(tweet_map)

    timeout = aiohttp.ClientTimeout(total=120) #sec
    async with aiohttp.ClientSession(timeout=timeout) as session:
        tasks = [
            convert_tweet(session, tweet_id, tweet, reply_graph, tweet_map, api, args)
            for tweet_id, tweet in tweet_map.items()
        ]
        results = await asyncio.gather(*tasks)

    # Process results
    for mal_formed_, download_failed_, api_failed_ in results:
        mal_formed += mal_formed_
        download_failed += download_failed_
        api_failed += api_failed_

    stats = {key: sum(1 for t in tweet_map.values() if t["type"] == key) for key in ["Post", "Reply", "Thread", "Retweet"]}
    console = Console()
    console.print("[bold green]Tweet Processing Summary:[/bold green]")
    total = 0
    for key, value in stats.items():
        console.print(f"{key}: {value}")
        total += value
    console.print(f"Total tweets: {total}")
    log.info(f"Malformed tweet replies: {mal_formed}")
    log.info(f"Download failed: {download_failed}")
    log.info(f"API failed: {api_failed}")

    end_time = time.perf_counter()
    total_time = end_time - start_time
    console.print(f"\nTotal time: {total_time:.2f} seconds")

if __name__ == "__main__":
    asyncio.run(main())
