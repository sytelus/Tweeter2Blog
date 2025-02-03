import os
import json
import logging
import argparse
import requests
from datetime import datetime, timezone
from collections import defaultdict
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

input_file = args.input
output_dir = args.output
user_id = args.user_id
os.makedirs(output_dir, exist_ok=True)

# Load tweets from JSON file
with open(input_file, "r", encoding="utf-8") as f:
    tweets_data = json.load(f)

tweet_map = {}
reply_graph = nx.DiGraph()

def convert_to_utc(dt_str):
    """Convert Twitter's created_at to UTC formatted datetime."""
    dt = datetime.strptime(dt_str, "%a %b %d %H:%M:%S +0000 %Y")
    return dt.replace(tzinfo=timezone.utc)

def classify_tweet(tweet):
    """Classify tweet as Post, Reply, or Thread."""
    text = tweet["full_text"].strip()
    if text.startswith("RT @"):
        return "Retweet"
    elif text.startswith("@"):  # Identifies replies
        return "Reply"
    elif tweet.get("in_reply_to_status_id_str") and tweet["in_reply_to_user_id_str"] == user_id:
        return "Thread"
    return "Post"

def extract_quoted_tweet_url(tweet):
    """Extract quoted tweet URL if present."""
    words = tweet["full_text"].split()
    if words and words[-1].startswith("https://t.co/"):
        return words[-1]
    return None

def resolve_twitter_url(url):
    """Resolve Twitter URL to its final redirected URL."""
    try:
        response = requests.head(url, allow_redirects=True)
        return response.url
    except requests.RequestException:
        return url

def generate_filename(tweet, prefix=""):
    """Generate a filename based on tweet creation time."""
    dt_utc = convert_to_utc(tweet["created_at"])
    return f"{prefix}{dt_utc.strftime('%Y%m%d%H%M')}"

def process_tweet(tweet):
    """Process each tweet, classify it, and build the reply graph."""
    tweet_id = tweet["id_str"]
    tweet_map[tweet_id] = tweet
    reply_graph.add_node(tweet_id, data=tweet)
    if tweet.get("in_reply_to_status_id_str"):
        reply_graph.add_edge(tweet["in_reply_to_status_id_str"], tweet_id)

def save_markdown(filename, content, subdir):
    """Save content as a markdown file in the specified directory."""
    folder_path = os.path.join(output_dir, subdir)
    os.makedirs(folder_path, exist_ok=True)
    file_path = os.path.join(folder_path, f"{filename}.md")
    with open(file_path, "w", encoding="utf-8") as f:
        f.write(content)
    log.info(f"Saved: {file_path}")

def format_markdown(tweet, thread=False):
    """Format tweet or thread as a markdown file."""
    content = []
    date_utc = convert_to_utc(tweet["created_at"]).isoformat()
    content.append(f"---\n")
    content.append(f"title: \"{tweet['full_text'][:50]}\"\n")
    content.append(f"draft: false\n")
    content.append(f"date: {date_utc}\n")
    content.append(f"slug: \"{tweet['id_str']}\"\n")
    content.append(f"---\n\n")

    text = tweet["full_text"]
    if text.startswith("RT @"):
        user = tweet["entities"]["user_mentions"][0]["screen_name"]
        content.append(f"Retweet of [@{user}](https://x.com/{user}):\n\n")

    if text.startswith("@"):  # Reply handling
        reply_to_id = tweet.get("in_reply_to_status_id_str")
        if reply_to_id:
            content.append(f"Replying to {{< twitter_simple id=\"{reply_to_id}\" >}}\n\n")

    quoted_url = extract_quoted_tweet_url(tweet)
    if quoted_url:
        resolved_url = resolve_twitter_url(quoted_url)
        if "https://x.com/" in resolved_url:
            tweet_id = resolved_url.split("status/")[-1]
            content.append(f"Quoted Tweet: {{< twitter_simple id=\"{tweet_id}\" >}}\n\n")

    if thread:
        for node in nx.dfs_preorder_nodes(reply_graph, source=tweet["id_str"]):
            content.append(tweet_map[node]["full_text"])
            content.append("\n\n")
    else:
        content.append(text)
    return "".join(content)

# Process tweets and build the graph
for item in track(tweets_data, description="Processing tweets..."):
    tweet = item["tweet"]
    tweet["user_id"] = tweet.get("user", {}).get("id_str", "")  # Ensure user ID is present
    process_tweet(tweet)

# Process and save tweets as markdown files
for tweet_id, tweet in track(tweet_map.items(), description="Saving tweets..."):
    tweet_type = classify_tweet(tweet)
    filename = generate_filename(tweet)

    markdown_content = format_markdown(tweet, thread=(tweet_type == "Thread"))
    save_markdown(filename, markdown_content, subdir=tweet_type.lower() + "s")

# Display final statistics
stats = {key: sum(1 for t in tweet_map.values() if classify_tweet(t) == key) for key in ["Post", "Reply", "Thread", "Retweet"]}
console.print("[bold green]Tweet Processing Summary:[/bold green]")
for key, value in stats.items():
    console.print(f"{key}: {value}")
