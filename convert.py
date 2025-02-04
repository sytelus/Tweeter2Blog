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

def extract_media_id(text):
    match = re.search(r"https://t\.co/([\w\d]+)$", text)
    return match.group(1) if match else None

def find_media_url(d, id_str):
    # Check if 'entities' exists in the dictionary
    if "entities" in d:
        entities = d["entities"]

        # Check if 'media' exists and is a list of dictionaries
        if "media" in entities and isinstance(entities["media"], list):
            for media_dict in entities["media"]:
                # Check if 'display_url' exists and ends with the given id_str
                if "display_url" in media_dict and media_dict["display_url"].endswith(id_str):
                    return media_dict["media_url_https"]

    # Return an empty string if no match is found
    return ""

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

def detect_media(tweet_map: Dict[str, Dict]) -> None:
    for tweet in tweet_map.values():
        content = tweet["full_text"]
        media_id = extract_media_id(content)

        tweet["media_filename"] = None
        tweet["image_alt"] = None
        tweet["media_url"] = None
        tweet["media_id"] = media_id

        if media_id:
            media_url = find_media_url(tweet, media_id)
            ext = os.path.splitext(urlparse(media_url).path)[-1]
            if media_url:
                media_filename = f"{media_id}{ext}"
                image_alt = ""
                tweet["media_filename"] = media_filename
                tweet["image_alt"] = image_alt
                tweet["media_url"] = media_url
            else:
                log.error(f"Media URL not found for {media_id}")
        # no media for this tweet

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
    detect_media(tweet_map)

    for tweet_id, tweet in track(tweet_map.items(), description="Saving tweets..."):
        tweet_type = tweet["type"]
        if tweet_type == "Thread":
            root_id = find_thread_root(tweet_id, reply_graph)
            if root_id != tweet_id:
                continue
            sequence = get_thread_sequence(root_id, tweet_map, reply_graph)
            thread_text = "\n\n".join([tweet_map[t]["full_text"] for t in sequence])
            tweet = tweet_map[root_id]
            tweet["full_text"] = thread_text

        storage_name = generate_storage_name(tweet)
        content_filepath = os.path.join(args.output, tweet["type"], storage_name + ".md")
        tweet["mark_down"] = tweet['full_text']
        if tweet["media_url"]:
            content_folder = os.path.join(args.output, tweet["type"], storage_name)
            os.makedirs(content_folder, exist_ok=True)
            content_filepath = os.path.join(content_folder, "index.md")
            error = download_image(tweet["media_url"], content_folder, tweet["media_filename"])
            if error:
                log.error(f"Error downloading image: {error}")
            else:
                tweet["mark_down"] = tweet['full_text'].replace(
                    f"https://t.co/{tweet['media_id']}",
                    f"\n\n![{tweet['image_alt']}]({tweet['media_filename']})")

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
