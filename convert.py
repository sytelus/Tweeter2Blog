from typing import Dict, List, Tuple, Mapping
import os
import json
import logging
import argparse
import requests
import re
from urllib.parse import urlparse
from datetime import datetime, timezone
from collections import defaultdict
from typing import Dict, List, Tuple
from rich.console import Console
from rich.progress import track
from rich.logging import RichHandler
import networkx as nx
import aiohttp
import asyncio

import model_api

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
    return parser.parse_args()

def convert_to_utc(dt_str: str) -> datetime:
    dt = datetime.strptime(dt_str, "%a %b %d %H:%M:%S +0000 %Y")
    return dt.replace(tzinfo=timezone.utc)

def find_thread_root(tweet_id: str, reply_graph: nx.DiGraph) -> str:
    while True:
        predecessors = list(reply_graph.predecessors(tweet_id))
        if not predecessors:
            return tweet_id
        tweet_id = predecessors[0]

def get_thread_sequence(root_id: str, tweet_map: Dict[str, Dict], reply_graph: nx.DiGraph) -> List[str]:
    tweets = [(t, convert_to_utc(tweet_map[t]["created_at"])) for t in nx.dfs_preorder_nodes(reply_graph, source=root_id)]
    return [t[0] for t in sorted(tweets, key=lambda x: x[1])]

def classify_tweets(tweet_map: Dict[str, Dict]) -> None:
    for tweet in tweet_map.values():
        text = tweet["full_text"].strip()
        assert "type" not in tweet

        if tweet.get("is_thread", ""):
            tweet["type"] = "Thread"
        elif text.startswith("RT @"):
            tweet["type"] = "Retweet"
        elif text.startswith("@"):
            tweet["type"] = "Reply"
        else:
            tweet["type"] = "Post"

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


def format_markdown(tweet: Dict) -> str:
    content = []
    date_utc = convert_to_utc(tweet["created_at"]).isoformat()
    content.append(f"---\n")
    content.append(f"title: \"{tweet['full_text'][:50]}\"\n")
    content.append(f"draft: false\n")
    content.append(f"date: {date_utc}\n")
    content.append(f"slug: \"{tweet['id_str']}\"\n")
    content.append(f"---\n\n")
    content.append(tweet["full_text"])
    return "".join(content)

def extract_twitter_urls(text):
    # This regular expression will find all occurrences of http://t.co/{id}
    pattern = r"https?://t\.co/[\w\d]+"

    # Find all non-overlapping matches in the text.
    matches = re.findall(pattern, text)
    return matches


def extract_tweet_info(url):
    # Regex breakdown:
    #   ^https://x\.com/       : URL must start with "https://x.com/"
    #   ([^/]+)               : Capture group for {user} (one or more characters not '/')
    #   /status/              : Literal string "/status/"
    #   ([^/?]+)              : Capture group for {id} (one or more characters not '/' or '?')
    #   (?:\?.*)?             : Optionally, a '?' followed by any characters (query parameters)
    #   $                     : End of string
    pattern = r"^https?://x\.com/([^/]+)/status/([^/?]+)(?:\?.*)?$"

    match = re.match(pattern, url)
    if match:
        user = match.group(1)
        tweet_id = match.group(2)
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
                tweet_info = extract_tweet_info(expanded)
                if tweet_info:
                    tweet_id, user = tweet_info
                    url_map[urls[0]] = tweet_shortcode(tweet_id, user)
                else:
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

def download_image(url, folder, filename):
    try:
        # Ensure the folder exists
        os.makedirs(folder, exist_ok=True)

        # Send a GET request to fetch the image
        response = requests.get(url, stream=True)
        response.raise_for_status()  # Raise an error for bad responses (4xx and 5xx)

        # Define the full path
        file_path = os.path.join(folder, filename)

        # Write the image content to the file
        with open(file_path, "wb") as file:
            for chunk in response.iter_content(1024):
                file.write(chunk)

        return file_path
    except requests.exceptions.HTTPError as e:
        # Check if the HTTP error is a 404 Not Found error
        if e.response is not None and e.response.status_code == 404:
            return None
        else:
            return e
    except Exception as e:
        return e

def id_from_url(url):
    pattern = r"https?://[^/]+/([^/]+)"
    match = re.search(pattern, url)
    return match.group(1) if match else None

def get_final_url(url, timeout=10):
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
                final_url = get_final_url(url)
                replacements[url] = { 'expanded': final_url }

        tweet["replacements"] = replacements

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

def _create_frontmatter(api, tweet, draft=True):
    date_utc = convert_to_utc(tweet["created_at"]).isoformat()
    # format string as YYYY-MM-DD-hhmm
    date_str = date_utc.replace(":", "-").replace("T", "-").split(".")[0].split("+")[0][:-2].replace("-", "")
    frontmatter = None
    if api.available:
        response = api.send_message(f"""
                        For below tweet, create a very short creatively funny but clever and informative title for the frontmatter to be used in blog and return it in the first line.
                        In the next line, create a short valid file name where this blog post can be saved.
                        Do not include anything else in your response.

                        {tweet['full_text']}""")


        # check if response is mapping type
        if isinstance(response, Mapping):
            if "choices" in response and len(response["choices"])>0 and "message" in response["choices"][0]:
                message = response["choices"][0]["message"]
                # separate two lines
                lines = message["content"].strip().split("\n")
                # ignore any blank lines
                lines = [line for line in lines if line]
                if len(lines) >= 2:
                    title = lines[0]
                    slug = lines[1]
                    if slug.endswith(".md"):
                        slug = slug[:-3]
                    # format frontmatter as markdown string
                    frontmatter = f"""---
title: "{title}"
draft: {str(draft).lower()}
date: {date_utc}
slug: "{date_str + '-' + slug}"
---

"""

    return frontmatter


def replace_twitter_handles(text):
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

def main() -> None:
    args = parse_arguments()
    mal_formed = 0
    download_failed = 0

    api = model_api.ModelAPI(enabled=False)

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
    classify_tweets(tweet_map)
    build_url_map(tweet_map)
    build_media_map(tweet_map)
    build_twittr_url_replacements(tweet_map)

    for tweet_id, tweet in track(tweet_map.items(), description="Saving tweets..."):
        tweet_type = tweet["type"]
        tweet["mark_down"] = '\n' + tweet['full_text'] + '\n'
        if tweet_type == "Thread": # club content of a thread
            root_id = find_thread_root(tweet_id, reply_graph)
            if root_id != tweet_id:
                continue
            sequence = get_thread_sequence(root_id, tweet_map, reply_graph)
            merged_replacements = {}
            for t in sequence:
                merged_replacements = merge_replacements(merged_replacements, tweet_map[t]["replacements"])
            thread_text = "\n\n".join([tweet_map[t]["full_text"] for t in sequence])
            tweet = tweet_map[root_id]
            tweet["full_text"] = thread_text
            tweet["replacements"] = merged_replacements
        elif tweet_type == "Reply": # replace @ with tweet link
            if 'in_reply_to_screen_name' in tweet and "in_reply_to_status_id_str" in tweet:
                reply_to_id = tweet["in_reply_to_status_id_str"]
                reply_to_user_id = tweet["in_reply_to_screen_name"]
                # get first word which should be the handle
                parts = tweet['mark_down'].strip().split(maxsplit=1)
                assert len(parts) == 2, f"First word and then rest of the test expcted: {tweet['mark_down']}"
                assert parts[0].startswith("@"), f"First word should be a handle: {parts[0]}"
                response_text = tweet_shortcode(reply_to_id, reply_to_user_id) + '\n\n' + parts[1]
                tweet["mark_down"] = response_text
            else:
                mal_formed += 1
            # else sometimes this info is missing and we can't do anything
        # else no other processing for other types

        storage_name = generate_storage_name(tweet)
        content_filepath = os.path.join(args.output, tweet["type"], storage_name + ".md")
        if tweet["replacements"]:
            for url, replacement in tweet["replacements"].items():
                if replacement.get("media_filename"):
                    content_folder = os.path.join(args.output, tweet["type"], storage_name)
                    os.makedirs(content_folder, exist_ok=True)
                    content_filepath = os.path.join(content_folder, "index.md")
                    filepath = download_image(replacement['expanded'], content_folder, replacement["media_filename"])
                    if filepath: # success
                        tweet["mark_down"] = tweet['mark_down'].replace(
                            url, f"\n\n![{replacement['image_alt']}]({replacement['media_filename']})")
                    else:
                        download_failed += 1
                        tweet["mark_down"] = tweet["mark_down"].replace(url, replacement["expanded"])
                else:
                    tweet["mark_down"] = tweet["mark_down"].replace(url, replacement["expanded"])
        tweet["mark_down"] = replace_twitter_handles(tweet["mark_down"])

        frontmatter = _create_frontmatter(api, tweet)
        if frontmatter:
            tweet["mark_down"] = frontmatter + tweet["mark_down"]

        os.makedirs(os.path.dirname(content_filepath), exist_ok=True)
        with open(content_filepath, "w", encoding="utf-8") as f:
            f.write(tweet["mark_down"])
        log.info(f"Saved: {content_filepath}")

    stats = {key: sum(1 for t in tweet_map.values() if t["type"] == key) for key in ["Post", "Reply", "Thread", "Retweet"]}
    console = Console()
    console.print("[bold green]Tweet Processing Summary:[/bold green]")
    for key, value in stats.items():
        console.print(f"{key}: {value}")
    log.info(f"Malformed tweet replies: {mal_formed}")
    log.info(f"Download failed: {download_failed}")
if __name__ == "__main__":
    main()
