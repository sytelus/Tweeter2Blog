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
console = Console()
logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",
    datefmt="[%X]",
    handlers=[RichHandler(console=console, rich_tracebacks=True)]
)
log = logging.getLogger("rich")

# Parse arguments
parser = argparse.ArgumentParser(description="Process tweets and generate markdown files.")
parser.add_argument("--input", required=True, help="Path to the input JSON file")
parser.add_argument("--output", required=True, help="Path to the output directory")
parser.add_argument("--user_id", required=True, help="User ID to identify threads")
args = parser.parse_args()

input_file: str = args.input
output_dir: str = args.output
user_id: str = args.user_id
os.makedirs(output_dir, exist_ok=True)

# Load tweets from JSON file
with open(input_file, "r", encoding="utf-8") as f:
    tweets_data: List[Dict] = json.load(f)

tweet_map: Dict[str, Dict] = {}
reply_graph: nx.DiGraph = nx.DiGraph()

def convert_to_utc(dt_str: str) -> datetime:
    """Convert Twitter's created_at to UTC formatted datetime."""
    dt = datetime.strptime(dt_str, "%a %b %d %H:%M:%S +0000 %Y")
    return dt.replace(tzinfo=timezone.utc)

def find_thread_root(tweet_id: str) -> str:
    """Find the root tweet of a thread by traversing up the reply chain."""
    while True:
        predecessors = list(reply_graph.predecessors(tweet_id))
        if not predecessors:
            return tweet_id
        tweet_id = predecessors[0]

def get_thread_sequence(root_id: str) -> List[str]:
    """Retrieve the tweets in a thread in the correct order based on creation time."""
    tweets: List[Tuple[str, datetime]] = [(t, convert_to_utc(tweet_map[t]["created_at"])) for t in nx.dfs_preorder_nodes(reply_graph, source=root_id)]
    return [t[0] for t in sorted(tweets, key=lambda x: x[1])]

def classify_tweet(tweet: Dict) -> str:
    """Classify tweet as Post, Reply, or Thread."""
    text: str = tweet["full_text"].strip()
    tweet_id: str = tweet["id_str"]

    if text.startswith("RT @"):
        return "Retweet"
    elif text.startswith("@"):  # Identifies replies
        return "Reply"
    elif tweet["user_id"] == user_id:
        if any(reply_graph.successors(tweet_id)) or any(reply_graph.predecessors(tweet_id)):
            return "Thread"
    return "Post"

def generate_filename(tweet: Dict, prefix: str = "") -> str:
    """Generate a filename based on tweet creation time."""
    dt_utc = convert_to_utc(tweet["created_at"])
    return f"{prefix}{dt_utc.strftime('%Y%m%d%H%M')}"

def process_tweet(tweet: Dict) -> None:
    """Process each tweet, classify it, and build the reply graph."""
    tweet_id: str = tweet["id_str"]
    tweet_map[tweet_id] = tweet
    reply_graph.add_node(tweet_id, data=tweet)
    if tweet.get("in_reply_to_status_id_str"):
        reply_graph.add_edge(tweet["in_reply_to_status_id_str"], tweet_id)

def save_markdown(filename: str, content: str, subdir: str) -> None:
    """Save content as a markdown file in the specified directory."""
    folder_path: str = os.path.join(output_dir, subdir)
    os.makedirs(folder_path, exist_ok=True)
    file_path: str = os.path.join(folder_path, f"{filename}.md")
    with open(file_path, "w", encoding="utf-8") as f:
        f.write(content)
    log.info(f"Saved: {file_path}")

def format_markdown(tweet: Dict) -> str:
    """Format tweet or thread as a markdown file."""
    content: List[str] = []
    date_utc: str = convert_to_utc(tweet["created_at"]).isoformat()
    content.append(f"---\n")
    content.append(f"title: \"{tweet['full_text'][:50]}\"\n")
    content.append(f"draft: false\n")
    content.append(f"date: {date_utc}\n")
    content.append(f"slug: \"{tweet['id_str']}\"\n")
    content.append(f"---\n\n")

    text: str = tweet["full_text"]
    content.append(text)
    return "".join(content)

# Process tweets and build the graph
for item in track(tweets_data, description="Processing tweets..."):
    tweet: Dict = item["tweet"]
    tweet["user_id"] = tweet.get("user", {}).get("id_str", "")  # Ensure user ID is present
    process_tweet(tweet)

# Process and save tweets as markdown files
for tweet_id, tweet in track(tweet_map.items(), description="Saving tweets..."):
    tweet_type: str = classify_tweet(tweet)
    if tweet_type == "Thread":
        root_id: str = find_thread_root(tweet_id)
        sequence: List[str] = get_thread_sequence(root_id)
        thread_text: str = "\n\n".join([tweet_map[t]["full_text"] for t in sequence])
        tweet = tweet_map[root_id]  # Use the first tweet as reference
        tweet["full_text"] = thread_text  # Update text with ordered thread content
        filename: str = generate_filename(tweet)
        markdown_content: str = format_markdown(tweet)
        save_markdown(filename, markdown_content, subdir="threads")
    else:
        filename: str = generate_filename(tweet)
        markdown_content: str = format_markdown(tweet)
        save_markdown(filename, markdown_content, subdir=tweet_type.lower() + "s")

# Display final statistics
stats: Dict[str, int] = {key: sum(1 for t in tweet_map.values() if classify_tweet(t) == key) for key in ["Post", "Reply", "Thread", "Retweet"]}
console.print("[bold green]Tweet Processing Summary:[/bold green]")
for key, value in stats.items():
    console.print(f"{key}: {value}")
