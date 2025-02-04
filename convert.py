import os
import json
import logging
import argparse
import requests
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

def generate_filename(tweet: Dict, prefix: str = "") -> str:
    dt_utc = convert_to_utc(tweet["created_at"])
    return f"{prefix}{dt_utc.strftime('%Y%m%d%H%M')}"

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

def save_markdown(filename: str, content: str, subdir: str, output_dir: str) -> None:
    folder_path = os.path.join(output_dir, subdir)
    os.makedirs(folder_path, exist_ok=True)
    file_path = os.path.join(folder_path, f"{filename}.md")
    with open(file_path, "w", encoding="utf-8") as f:
        f.write(content)
    log.info(f"Saved: {file_path}")

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
            filename = generate_filename(tweet)
            markdown_content = format_markdown(tweet)
            save_markdown(filename, markdown_content, "threads", args.output)
        else:
            filename = generate_filename(tweet)
            markdown_content = format_markdown(tweet)
            save_markdown(filename, markdown_content, tweet_type.lower() + "s", args.output)
    stats = {key: sum(1 for t in tweet_map.values() if t["type"] == key) for key in ["Post", "Reply", "Thread", "Retweet"]}
    console = Console()
    console.print("[bold green]Tweet Processing Summary:[/bold green]")
    for key, value in stats.items():
        console.print(f"{key}: {value}")

if __name__ == "__main__":
    main()
