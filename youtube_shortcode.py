import re
from urllib.parse import urlparse, parse_qs

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
            return "{{< youtube " + video_id + " >}}"
        else:
            return match.group(0)

    return re.sub(pattern, replace, text)

# Example usage:
if __name__ == "__main__":
    sample_text = (
        "Here is a YouTube video: <https://www.youtube.com/watch?v=abc123XYZ> and another one: "
        "<https://youtu.be/def456UVW>. This one is not a YouTube URL: <https://example.com> or this one https://www.youtube.com/watch?v=abc123XYZ"
    )
    converted_text = youtube_to_shortcode(sample_text)
    print(converted_text)
