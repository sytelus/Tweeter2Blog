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
    pattern = r"https://t\.co/[\w\d]+"

    # Find all non-overlapping matches in the text.
    matches = re.findall(pattern, text)
    return matches

def build_url_map(tweet_map: Dict[str, Dict]):
    url_map = {}
    for tweet in tweet_map.values():
        content = tweet["full_text"]
        if "entities" in tweet and "urls" in tweet["entities"]:
            for url_dict in tweet["entities"]["urls"]:
                urls = extract_twitter_urls(url_dict["url"])
                assert len(urls) == 1, f"Expected 1 URL, found {len(urls)} in {url_dict['url']}"
                url_map[urls[0]] = url_dict["expanded_url"]
        tweet["url_map"] = url_map

def build_media_map(tweet_map: Dict[str, Dict]):
    media_map = {}
    for tweet in tweet_map.values():
        content = tweet["full_text"]
        if "entities" in tweet and "media" in tweet["entities"]:
            for media_dict in tweet["entities"]["media"]:
                urls = extract_twitter_urls(media_dict["url"])
                assert len(urls) == 1, f"Expected 1 URL, found {len(urls)} in {media_dict['url']}"
                media_map[urls[0]] = media_dict["media_url_https"]
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

        return None
    except Exception as e:
        return e

def id_from_url(url):
    pattern = r"https://[^/]+/([^/]+)"
    match = re.search(pattern, url)
    return match.group(1) if match else None

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
                raise RuntimeError(f"URL not found in media or url map: {url}")

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

def replace_twitter_handles(text):
    # Using (?<!\S) ensures that the character before '@' is not a non-whitespace character,
    # i.e. it's either the start of the string or a whitespace.
    pattern = r"(?<!\S)@(\w+)"

    def repl(match):
        handle = match.group(1)
        # Replace with markdown formatted link: [handle](https://x.com/handle)
        return f"[{handle}](https://x.com/{handle})"

    return re.sub(pattern, repl, text)

def main() -> None:
    args = parse_arguments()
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

        storage_name = generate_storage_name(tweet)
        content_filepath = os.path.join(args.output, tweet["type"], storage_name + ".md")
        tweet["mark_down"] = '\n' + tweet['full_text'] + '\n'
        if tweet["replacements"]:
            for url, replacement in tweet["replacements"].items():
                if replacement.get("media_filename"):
                    content_folder = os.path.join(args.output, tweet["type"], storage_name)
                    os.makedirs(content_folder, exist_ok=True)
                    content_filepath = os.path.join(content_folder, "index.md")
                    error = download_image(replacement['expanded'], content_folder, replacement["media_filename"])
                    assert not error, f"Error downloading image: {error}"
                    tweet["mark_down"] = tweet['mark_down'].replace(
                        url, f"\n\n![{replacement['image_alt']}]({replacement['media_filename']})")
                else:
                    tweet["mark_down"] = tweet["mark_down"].replace(url, replacement["expanded"])
        tweet["mark_down"] = replace_twitter_handles(tweet["mark_down"])

        os.makedirs(os.path.dirname(content_filepath), exist_ok=True)
        with open(content_filepath, "w", encoding="utf-8") as f:
            f.write(tweet["mark_down"])
        log.info(f"Saved: {content_filepath}")

    stats = {key: sum(1 for t in tweet_map.values() if t["type"] == key) for key in ["Post", "Reply", "Thread", "Retweet"]}
    console = Console()
    console.print("[bold green]Tweet Processing Summary:[/bold green]")
    for key, value in stats.items():
        console.print(f"{key}: {value}")

if __name__ == "__main__":
    main()
