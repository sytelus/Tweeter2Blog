import os
import json
import logging
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

# Load tweets from JSON file
input_file = "sample.json"
output_dir = "output"
os.makedirs(output_dir, exist_ok=True)

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
    if tweet.get("retweeted"):
        return "Retweet"
    elif tweet.get("in_reply_to_status_id_str"):
        if tweet["in_reply_to_user_id_str"] == tweet["user_id"]:
            return "Thread"
        return "Reply"
    return "Post"

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

    if thread:
        for node in nx.dfs_preorder_nodes(reply_graph, source=tweet["id_str"]):
            content.append(tweet_map[node]["full_text"])
            content.append("\n\n")
    else:
        content.append(tweet["full_text"])
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

    if tweet_type == "Retweet" and not tweet.get("is_quote_status", False):
        continue  # Skip retweets without quotes

    markdown_content = format_markdown(tweet, thread=(tweet_type == "Thread"))
    save_markdown(filename, markdown_content, subdir=tweet_type.lower() + "s")

# Display final statistics
stats = {
    "Posts": sum(1 for t in tweet_map.values() if classify_tweet(t) == "Post"),
    "Replies": sum(1 for t in tweet_map.values() if classify_tweet(t) == "Reply"),
    "Threads": sum(1 for t in tweet_map.values() if classify_tweet(t) == "Thread"),
    "Retweets": sum(1 for t in tweet_map.values() if classify_tweet(t) == "Retweet"),
}
console.print("[bold green]Tweet Processing Summary:[/bold green]")
for key, value in stats.items():
    console.print(f"{key}: {value}")
