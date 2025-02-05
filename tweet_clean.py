import re

def parse_text(text: str):
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

def main():
    # Some test cases including edge cases:
    test_cases = [
        # 1. Simple case with one URL.
        "This is a \n\nsimple test...\n http://example.com",
        "This is a \n\nsimple test... http://example.com",
        "This is a ...simple test...\n http://example.com",


        # 2. Multi-line text with multiple URLs.
        "Multi-line example:\nLine two of the text\n...\t http://a.com https://b.org?k=3",

        # 3. Pattern with no URLs (but note that at least one whitespace is required after the dots).
        "Another test case with no URLs...   ",

        # 4. No matching pattern because the three dots are not followed by whitespace.
        "This text does not match...http://example.com",

        # 5. No pattern at all.
        "Just some random text that does not end with the required pattern",
        "Just some random text that ... does not end with the required pattern http://example.com",
        "Just some random text that ... does not end with the required pattern",

        # 6. Edge case: the entire string is just the pattern with empty prefix and no URLs.
        "...   ",

        # 7. Edge case: prefix with newlines.
        "Line one\nLine two\nLine three...\n   https://site.com",
    ]

    for idx, text in enumerate(test_cases, 1):
        found, prefix, urls = parse_text(text)
        print(f"Test Case {idx}:")
        print("Input:")
        print(repr(text))
        print("Pattern found:", found)
        print("Prefix:", repr(prefix))
        print("URLs:", urls)
        print("-" * 40)

if __name__ == "__main__":
    main()
