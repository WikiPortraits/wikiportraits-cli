import requests
import argparse
import sys
from datetime import datetime
import json
from typing import Dict, List, Optional, Set, Generator, Tuple
from collections import defaultdict
from wikidata_utils import (
    get_wikidata_item_for_page,
    get_current_image_for_item,
    get_image_history_for_item,
    check_if_file_matches_wikidata_image,
    P18_IMAGE
)
from wiki_api_client import WikiAPIClient, WikiAPIError

# API endpoints
COMMONS_API: str = "https://commons.wikimedia.org/w/api.php"
WIKI_API_TEMPLATE: str = "https://{wiki}/w/api.php"
WIKIDATA_API: str = "https://www.wikidata.org/w/api.php"
USER_AGENT: str = "WikiPortraits (https//www.wikiportraits.org; hello@wikiportraits.org)"

IMAGE_EXTENSIONS: Set[str] = {'.jpg', '.jpeg', '.png', '.gif', '.tiff'}
MAX_PREV_IMAGES = 5

# Formatting constants
TABLE_WIDTH = 90
COLUMN_WIDTHS = {
    'index': 3,
    'language': 8,
    'total_uses': 12,
    'percent': 10,
    'first_images': 15,
    'wikidata': 15,
    'file': 50,
    'uses': 12
}
DIVIDER_LINE = "─" * TABLE_WIDTH
HEADER_LINE = "═" * TABLE_WIDTH

class StatisticsTracker:
    """Class to track and manage statistics about image usage"""
    def __init__(self):
        self.files_used_on_wikipedias = 0
        self.wiki_pages_seen = defaultdict(set)
        self.usage_by_wiki = defaultdict(int)
        self.first_image_by_file = defaultdict(int)
        self.total_usages_by_file = defaultdict(int)
        self.first_image_by_wiki = defaultdict(int)
        self.total_usages_by_wiki = defaultdict(int)
        self.first_image_by_lang = defaultdict(int)
        self.total_usages_by_lang = defaultdict(int)
        self.wikidata_sourced_count = 0
        self.wikidata_sourced_by_wiki = defaultdict(int)
        self.first_p18_count = 0
        self.replaced_p18_count = 0

    def update_file_usage(self, file_title: str, wiki: str, lang_code: str, is_first_image: bool, from_wikidata: bool, is_first_p18: bool) -> None:
        """Update statistics for a single file usage"""
        self.total_usages_by_file[file_title] += 1
        self.total_usages_by_wiki[wiki] += 1
        self.total_usages_by_lang[lang_code] += 1
        
        if is_first_image:
            self.first_image_by_file[file_title] += 1
            self.first_image_by_wiki[wiki] += 1
            self.first_image_by_lang[lang_code] += 1
        
        if from_wikidata:
            self.wikidata_sourced_count += 1
            self.wikidata_sourced_by_wiki[wiki] += 1
            
            if is_first_p18:
                self.first_p18_count += 1
            else:
                self.replaced_p18_count += 1

    def get_wikidata_count_for_language(self, lang_code: str) -> int:
        """Get the total Wikidata-sourced count for a language"""
        return sum(
            self.wikidata_sourced_by_wiki.get(wiki, 0)
            for wiki in self.usage_by_wiki.keys()
            if get_language_code(wiki) == lang_code
        )

def get_commons_category_files(category_name: str, depth: int = 0) -> List[str]:
    """Returns a list of file names in the given Commons category and optionally its subcategories.
    
    Args:
        category_name: Name of the category without "Category:" prefix
        depth: How many levels deep to recurse into subcategories (0 = no recursion)
    
    Returns:
        List of file titles (including "File:" prefix)
    
    Raises:
        WikiAPIError: If the API request fails
    """
    all_files: List[str] = []
    processed_categories: Set[str] = set()
    
    def get_category_files_recursive(cat_name: str, current_depth: int) -> None:
        """Recursively get files from a category and its subcategories."""
        if current_depth > depth or cat_name in processed_categories:
            return
        
        processed_categories.add(cat_name)
        client = WikiAPIClient(USER_AGENT)
        
        files_params = {
            "action": "query",
            "list": "categorymembers",
            "cmtitle": f"Category:{cat_name}",
            "cmtype": "file",
            "cmlimit": "max",
            "format": "json"
        }
        
        print(f"{'  ' * current_depth}Fetching files from Commons category: {cat_name} (depth {current_depth})")
        
        try:
            while True:
                data = client.make_request(COMMONS_API, files_params)
                
                categorymembers = data.get("query", {}).get("categorymembers", [])
                category_files = [member["title"] for member in categorymembers]
                all_files.extend(category_files)
                
                continue_params = client.get_continue_params(data)
                if not continue_params:
                    break
                files_params.update(continue_params)
            
            print(f"{'  ' * current_depth}Found {len([f for f in category_files if f.startswith('File:')])} file(s) in '{cat_name}'")
            
            # If we haven't reached max depth, get subcategories
            if current_depth < depth:
                subcats_params = {
                    "action": "query",
                    "list": "categorymembers",
                    "cmtitle": f"Category:{cat_name}",
                    "cmtype": "subcat",
                    "cmlimit": "max",
                    "format": "json"
                }
                
                subcategories = []
                while True:
                    data = client.make_request(COMMONS_API, subcats_params)
                    
                    categorymembers = data.get("query", {}).get("categorymembers", [])
                    batch_subcats = [member["title"].removeprefix("Category:") for member in categorymembers]
                    subcategories.extend(batch_subcats)
                    
                    continue_params = client.get_continue_params(data)
                    if not continue_params:
                        break
                    subcats_params.update(continue_params)
                
                if subcategories:
                    print(f"{'  ' * current_depth}Found {len(subcategories)} subcategory(ies) in '{cat_name}'")
                    for subcat in subcategories:
                        get_category_files_recursive(subcat, current_depth + 1)
        
        except WikiAPIError as e:
            print(f"Error fetching category members from '{cat_name}': {e}", file=sys.stderr)
    
    get_category_files_recursive(category_name, 0)
    
    # Remove duplicates while preserving order
    unique_files = []
    seen = set()
    for file in all_files:
        if file not in seen and file.startswith('File:'):
            unique_files.append(file)
            seen.add(file)
    
    total_categories = len(processed_categories)
    print(f"\nProcessed {total_categories} categor{'ies' if total_categories != 1 else 'y'} total")
    print(f"Found {len(unique_files)} unique file(s) across all categories")
    
    return unique_files

def get_global_usage_of_file(filename: str) -> Dict[str, List[str]]:
    """Returns a dictionary of wiki domains and page titles where 'filename' is used.
    
    Args:
        filename: Full file title including "File:" prefix
    
    Returns:
        Dictionary with keys as wiki domains (e.g., 'en.wikipedia.org') and values
        as lists of page titles on that wiki where the file is used
    
    Raises:
        WikiAPIError: If the API request fails
    """
    wiki_pages: Dict[str, Set[str]] = defaultdict(set)
    client = WikiAPIClient(USER_AGENT)
    
    params = {
        "action": "query",
        "prop": "globalusage",
        "titles": filename,
        "gulimit": "max",
        "format": "json",
        "gunamespace": "0" # 0 = mainspace
    }
    
    try:
        while True:
            data = client.make_request(COMMONS_API, params)
            
            pages_dict = data.get("query", {}).get("pages", {})
            for page_data in pages_dict.values():
                for usage in page_data.get("globalusage", []):
                    wiki = usage.get("wiki")
                    if wiki and wiki.endswith('.wikipedia.org'):
                        wiki_pages[wiki].add(usage["title"])
            
            continue_params = client.get_continue_params(data)
            if not continue_params:
                break
            params.update(continue_params)
        
        # Convert sets to lists for JSON
        return {wiki: list(pages) for wiki, pages in wiki_pages.items()}
        
    except WikiAPIError as e:
        print(f"Error fetching global usage: {e}", file=sys.stderr)
        return {}

def get_page_revisions_wikitext_descending(wiki: str, page_title: str) -> Generator[Tuple[int, str, str], None, None]:
    """Generator that yields (revision_id, timestamp, wikitext) for all revisions of 'page_title'.
    
    Args:
        wiki: The wiki domain (e.g., 'en.wikipedia.org')
        page_title: The Wikipedia page title
    
    Yields:
        Tuple of (revision_id, timestamp, wikitext)
    
    Raises:
        WikiAPIError: If the API request fails
    """
    client = WikiAPIClient(USER_AGENT)
    api_url = WIKI_API_TEMPLATE.format(wiki=wiki)
    
    params = {
        "action": "query",
        "prop": "revisions",
        "titles": page_title,
        "rvprop": "ids|timestamp|content",
        "rvslots": "main",
        "rvlimit": "max",
        "format": "json",
        "rvdir": "older"
    }
    
    print(f"Fetching revisions for page '{page_title}' on {wiki} (newest -> oldest)")
    
    try:
        while True:
            data = client.make_request(api_url, params)
            
            pages = data.get("query", {}).get("pages", {})
            for page_data in pages.values():
                for rev in page_data.get("revisions", []):
                    revision_id = rev.get("revid")
                    timestamp = rev.get("timestamp")
                    content = rev.get("slots", {}).get("main", {}).get("*", "")
                    if revision_id and timestamp and content:
                        yield revision_id, timestamp, content
            
            continue_params = client.get_continue_params(data)
            if not continue_params:
                break
            params.update(continue_params)
            
    except WikiAPIError as e:
        print(f"Error fetching revisions from {wiki}: {e}", file=sys.stderr)
        return

def has_image_in_wikitext(content: str) -> bool:
    """Checks if wikitext contains an image by looking for common image-related patterns.
    
    Args:
        content: The wikitext content to check
    
    Returns:
        True if an image is detected, False otherwise
    """
    content_lower = content.lower()

    has_extension = any(ext in content_lower for ext in IMAGE_EXTENSIONS)
    
    return has_extension

def matches_file_in_wikitext(content: str, file_name: str) -> bool:
    """Checks if 'file_name' appears in 'content', ignoring case and treating underscores and spaces as interchangeable.
    
    Args:
        content: The wikitext content to check
        file_name: The file name to look for
    
    Returns:
        True if the file is present, False otherwise
    """
    content_lower = content.lower()
    file_name_lower = file_name.lower().removeprefix("file:")
    
    fn_spaces = file_name_lower.replace("_", " ")
    fn_underscores = file_name_lower.replace(" ", "_")
    
    return any(variant in content_lower for variant in {file_name_lower, fn_spaces, fn_underscores})

def find_earliest_introduction(wiki: str, page_title: str, file_name: str) -> Optional[Dict]:
    """Iterates through page revisions to find when 'file_name' was first added and whether it was the first image.
    
    Args:
        wiki: The wiki domain (e.g., 'en.wikipedia.org')
        page_title: The Wikipedia page title
        file_name: The file name to look for
    
    Returns:
        Dict containing introduction info or None if file never found
    """
    print(f"Checking earliest introduction for file '{file_name}' on {wiki} page '{page_title}'")
    
    # Check if the file appears directly in the wikitext
    wikitext_info = find_earliest_wikitext_introduction(wiki, page_title, file_name)
    if wikitext_info:
        return wikitext_info
    
    # If not found, check if it's sourced from Wikidata (i.e. automated infobox)
    print(f"File '{file_name}' not found directly in wikitext. Checking Wikidata...")
    return find_wikidata_introduction(wiki, page_title, file_name)

def find_earliest_wikitext_introduction(wiki: str, page_title: str, file_name: str) -> Optional[Dict]:
    """Find when a file was first added to a page's wikitext and whether it was the first image.
    
    Args:
        wiki: The wiki domain
        page_title: The Wikipedia page title
        file_name: The file name to look for
    
    Returns:
        Dict containing introduction info or None if file not found in wikitext
    """
    earliest_rev_id = None
    earliest_ts = None
    found_any = False
    
    for rev_id, ts, content in get_page_revisions_wikitext_descending(wiki, page_title):
        has_target_file = matches_file_in_wikitext(content, file_name)
        
        if not found_any:
            if has_target_file:
                found_any = True
                earliest_rev_id = rev_id
                earliest_ts = ts
                print(f"Found '{file_name}' in wikitext at revision {rev_id} ({ts})")
        else:
            if not has_target_file:
                # Check if any images existed before
                first_image = not has_image_in_wikitext(content)
                
                print(
                    f"File '{file_name}' introduced in rev {earliest_rev_id} "
                    f"({earliest_ts}). First image on page? {first_image}"
                )
                
                return {
                    "introduced_revision_id": earliest_rev_id,
                    "introduced_timestamp": earliest_ts,
                    "first_image": first_image,
                    "from_wikidata": False
                }
            else:
                earliest_rev_id = rev_id
                earliest_ts = ts
    
    # If the file is present in the oldest revision, it's the first image
    if found_any:
        first_image = True
        
        print(
            f"File '{file_name}' present from earliest known revision on '{page_title}'. "
            f"Introduced rev: {earliest_rev_id} ({earliest_ts}), First image? {first_image}"
        )
        
        return {
            "introduced_revision_id": earliest_rev_id,
            "introduced_timestamp": earliest_ts,
            "first_image": first_image,
            "from_wikidata": False
        }
    
    return None

def find_wikidata_introduction(wiki: str, page_title: str, file_name: str) -> Optional[Dict]:
    """Find when a file was first added to a page via Wikidata and whether it was the first image.
    
    Args:
        wiki: The wiki domain
        page_title: The Wikipedia page title
        file_name: The file name to look for
    
    Returns:
        Dict containing introduction info or None if file not found in Wikidata
    """
    # Get Wikidata item for page
    wikidata_item = get_wikidata_item_for_page(wiki, page_title, USER_AGENT)
    if not wikidata_item:
        print(f"No Wikidata item found for {wiki} page '{page_title}'")
        return None
    
    # Get current image
    wikidata_image = get_current_image_for_item(wikidata_item, USER_AGENT)
    if not wikidata_image:
        print(f"No image (P18) property found for Wikidata item {wikidata_item}")
        return None
    
    # Check if file matches Wikidata
    if check_if_file_matches_wikidata_image(file_name, wikidata_image):
        print(f"Found '{file_name}' in Wikidata item {wikidata_item} as property P18")

        # Determine if first image or replaced another
        try:
            image_history = get_image_history_for_item(wikidata_item, USER_AGENT)
            
            # Validate image_history structure
            if not isinstance(image_history, list):
                raise ValueError(f"Invalid image_history type: {type(image_history)}")
            
            if image_history and not isinstance(image_history[0], dict):
                raise ValueError(f"Invalid image_history structure: first element is {type(image_history[0])}")
            
            first_p18 = len(image_history) <= 1
            
            if first_p18:
                print(f"This appears to be the first P18 image value for item {wikidata_item}")
            else:
                print(f"This image replaced {len(image_history)-1} previous P18 value(s) for item {wikidata_item}")
                if len(image_history) > 1:
                    prev_images = [h['image'] for h in image_history[1:] if isinstance(h, dict) and 'image' in h]
                    print(f"Previous image(s): {', '.join(prev_images)}")

            timestamp_str = image_history[0]["timestamp"] if image_history else "Unknown"
            previous_images = [h["image"] for h in image_history[1:] if isinstance(h, dict) and "image" in h] if len(image_history) > 1 else []

            return {
                "introduced_revision_id": image_history[0].get("revision_id", 0) if image_history else 0,
                "introduced_timestamp": timestamp_str,
                "timestamp_is_formatted": True,
                "first_image": len(previous_images) == 0, 
                "first_p18": first_p18,
                "from_wikidata": True,
                "wikidata_item": wikidata_item,
                "previous_p18_images": previous_images
            }
            
        except Exception as e:
            print(f"Error processing image history for item {wikidata_item}: {e}")
            raise
    
    print(f"File '{file_name}' not found in Wikidata item {wikidata_item} either.")
    return None

def format_timestamp(timestamp: str) -> str:
    """Convert API timestamp to readable format.
    
    Args:
        timestamp: The API timestamp string
    
    Returns:
        Formatted timestamp string
    """
    dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
    return dt.strftime('%Y-%m-%d %H:%M:%S UTC')

def get_language_code(wiki_domain: str) -> str:
    """Extract language code from wiki domain.
    
    Args:
        wiki_domain: The wiki domain (e.g., 'en.wikipedia.org')
    
    Returns:
        The language code (e.g., 'en') or 'unknown' if not a Wikipedia domain
    """
    if not wiki_domain.endswith('.wikipedia.org'):
        return 'unknown'
    return wiki_domain.split('.')[0]

def get_percentage(part: int, total: int) -> str:
    """Format a percentage with 1 decimal place.
    
    Args:
        part: The part value
        total: The total value
    
    Returns:
        Formatted percentage string with 1 decimal place
    """
    if total == 0:
        return "0.0%"
    return f"{part/total*100:.1f}%"

def format_number(number: int, total: Optional[int] = None, width: int = 0, style: str = "number") -> str:
    """Format a number with optional percentage or other styles, adjusting spacing based on available width.
    
    Args:
        number: The number to format
        total: Optional total for percentage calculation
        width: Available width for the formatted string
        style: Formatting style:
            - "number": Just the number
            - "percentage": Number with percentage of total
            - "count": Number with total in parentheses
            - "ratio": Number/total format
    
    Returns:
        Formatted string that fits within the specified width
    """
    if style == "number" or not total:
        return str(number)
    
    number_str = str(number)
    total_str = str(total)
    
    if style == "percentage":
        percentage = get_percentage(number, total)
        full_str = f"{number_str} ({percentage})"
    elif style == "count":
        full_str = f"{number_str} ({total_str})"
    elif style == "ratio":
        full_str = f"{number_str}/{total_str}"
    else:
        return number_str
    
    return full_str if width == 0 or len(full_str) <= width else number_str

def get_sorted_files_by_first_image(stats: StatisticsTracker) -> List[Tuple[str, int, int]]:
    """Get files sorted by first image percentage.
    
    Args:
        stats: StatisticsTracker instance
        
    Returns:
        List of tuples (file, first_count, total_count) sorted by first image percentage
    """
    return sorted(
        [(file, stats.first_image_by_file[file], stats.total_usages_by_file[file]) 
         for file in stats.total_usages_by_file.keys()],
        key=lambda x: (x[1], x[1]/x[2] if x[2] > 0 else 0),
        reverse=True
    )

def get_sorted_files_by_usage(stats: StatisticsTracker) -> List[Tuple[str, int]]:
    """Get files sorted by total usage count.
    
    Args:
        stats: StatisticsTracker instance
        
    Returns:
        List of tuples (file, total_count) sorted by total usage
    """
    return sorted(
        [(file, stats.total_usages_by_file[file]) 
         for file in stats.total_usages_by_file.keys()],
        key=lambda x: x[1],
        reverse=True
    )[:10]

def get_sorted_languages(stats: StatisticsTracker) -> List[Tuple[str, int, int]]:
    """Get languages sorted by total usage.
    
    Args:
        stats: StatisticsTracker instance
        
    Returns:
        List of tuples (lang, first_count, total_count) sorted by total usage
    """
    return sorted(
        [(lang, stats.first_image_by_lang[lang], stats.total_usages_by_lang[lang]) 
         for lang in stats.total_usages_by_lang.keys()],
        key=lambda x: x[2],
        reverse=True
    )

def print_table_header(title: str, columns: List[Tuple[str, int]], divider: str = DIVIDER_LINE) -> None:
    """Print a formatted table header with column titles.
    
    Args:
        title: The table title
        columns: List of (column_name, width) tuples
        divider: The divider line to use (defaults to DIVIDER_LINE)
    """
    print(f"\n{divider}")
    print(title)
    print(divider)
    
    # Print column headers
    header = "".join(f"{name:<{width}}" for name, width in columns)
    print(header)
    print(divider)

def truncate_filename(filename: str, max_length: int) -> str:
    """Truncate a filename to fit within a specified length.
    
    Args:
        filename: The filename to truncate
        max_length: Maximum length of the truncated filename
        
    Returns:
        Truncated filename with ellipsis if needed
    """
    if len(filename) <= max_length:
        return filename
    return filename[:max_length - 3] + "..."

def print_summary_statistics(results: List[Dict], stats: StatisticsTracker, category: str, 
                           start_time: datetime, total_files_in_category: int) -> None:
    """Print summary statistics about image usage across Wikipedias.
    
    Args:
        results: List of dictionaries containing detailed usage information
        stats: StatisticsTracker instance with aggregated statistics
        category: Name of the Commons category being analyzed
        start_time: When the analysis started
        total_files_in_category: Total number of files in the category
    """
    total_usages = len(results)
    
    # Count unique pages across all wikis
    unique_pages_per_wiki = {wiki: len(pages) for wiki, pages in stats.wiki_pages_seen.items()}
    total_unique_pages = sum(unique_pages_per_wiki.values())
    
    # Count first image occurrences
    first_image_count = sum(1 for r in results if r["first_image"])
    not_first_count = total_usages - first_image_count
    
    # Count wikis with usage
    wikis_with_usage = len(stats.wiki_pages_seen)
    
    # Print summary statistics
    print(f"\n{HEADER_LINE}")
    print(f"📊 SUMMARY STATISTICS FOR CATEGORY: {category}")
    print(HEADER_LINE)
    
    print(f"🖼️ Total files in category:        {total_files_in_category:>6}")
    print(f"🔖 Files used on Wikipedias:       {stats.files_used_on_wikipedias:>6} ({get_percentage(stats.files_used_on_wikipedias, total_files_in_category)})")
    print(f"🗣️ Wikipedia languages with usage: {wikis_with_usage:>6}")
    print(f"📊 Total usage instances:          {total_usages:>6}")
    print(f"📄 Distinct Wikipedia pages:       {total_unique_pages:>6}")
    
    print(f"\n{DIVIDER_LINE}")
    print("🔍 WIKIPEDIA USAGE STATISTICS (across all languages)")
    print(DIVIDER_LINE)
    print(f"🥇 First-time image added to page: {first_image_count:>6} ({get_percentage(first_image_count, total_usages)})")
    print(f"➕ Not first image:                {not_first_count:>6} ({get_percentage(not_first_count, total_usages)})")
    print(f"🔄 Sourced from Wikidata P18:      {stats.wikidata_sourced_count:>6} ({get_percentage(stats.wikidata_sourced_count, total_usages)})")
    
    if stats.wikidata_sourced_count > 0:
        print(f"\n{DIVIDER_LINE}")
        print("🔍 WIKIDATA P18 STATISTICS")
        print(DIVIDER_LINE)
        print(f"✨ First P18 value:               {stats.first_p18_count:>6} ({get_percentage(stats.first_p18_count, stats.wikidata_sourced_count)})")
        print(f"🔄 Replaced previous P18:         {stats.replaced_p18_count:>6} ({get_percentage(stats.replaced_p18_count, stats.wikidata_sourced_count)})")
        print(f"📊 Total Wikidata-sourced:        {stats.wikidata_sourced_count:>6}")
    
    files_with_data = get_sorted_files_by_first_image(stats)
    if files_with_data:
        print(f"\n{DIVIDER_LINE}")
        print(f"🥇 TOP-10 FIRST-TIME IMAGES ON ARTICLES ACROSS ALL LANGUAGES")
        print(DIVIDER_LINE)
        
        print(f"{'#':<{COLUMN_WIDTHS['index']}} {'File':<{COLUMN_WIDTHS['file']}} {'First Uses':<{COLUMN_WIDTHS['uses']}} {'Total Uses':<{COLUMN_WIDTHS['uses']}} {'Percentage':<{COLUMN_WIDTHS['percent']}}")
        print(DIVIDER_LINE)
        
        for i, (file, first_count, total_count) in enumerate(files_with_data[:10]):
            short_file = truncate_filename(file, COLUMN_WIDTHS['file'] - 2)
            print(f"{i+1:<{COLUMN_WIDTHS['index']}} {short_file:<{COLUMN_WIDTHS['file']}} {first_count:<{COLUMN_WIDTHS['uses']}} {total_count:<{COLUMN_WIDTHS['uses']}} {get_percentage(first_count, total_count):<{COLUMN_WIDTHS['percent']}}")
    
    most_used_files = get_sorted_files_by_usage(stats)
    if most_used_files:
        print(f"\n{DIVIDER_LINE}")
        print(f"🥇 TOP-10 MOST USED IMAGES ACROSS ALL LANGUAGES")
        print(DIVIDER_LINE)
        
        print(f"{'#':<{COLUMN_WIDTHS['index']}} {'File':<{COLUMN_WIDTHS['file']}} {'Total Uses':<{COLUMN_WIDTHS['uses']}} {'% of All':<{COLUMN_WIDTHS['percent']}}")
        print(DIVIDER_LINE)
        
        for i, (file, total_count) in enumerate(most_used_files, start=1):
            short_file = truncate_filename(file, COLUMN_WIDTHS['file'] - 2)
            print(f"{i:<{COLUMN_WIDTHS['index']}} {short_file:<{COLUMN_WIDTHS['file']}} {total_count:<{COLUMN_WIDTHS['uses']}} {get_percentage(total_count, total_usages):<{COLUMN_WIDTHS['percent']}}")
    
    langs_with_data = get_sorted_languages(stats)
    if langs_with_data:
        print(f"\n{DIVIDER_LINE}")
        print("🗣️ WIKIPEDIA LANGUAGE ANALYSIS")
        print(DIVIDER_LINE)
        
        print(f"{'#':<{COLUMN_WIDTHS['index']}} {'Language':<{COLUMN_WIDTHS['language']}} {'Total Uses':<{COLUMN_WIDTHS['total_uses']}} {'% of All':<{COLUMN_WIDTHS['percent']}} {'First Images':<{COLUMN_WIDTHS['first_images']}} {'Via Wikidata':<{COLUMN_WIDTHS['wikidata']}}")
        print(DIVIDER_LINE)
        
        for i, (lang, first_count, total_count) in enumerate(langs_with_data, start=1):
            wikidata_count = stats.get_wikidata_count_for_language(lang)
            
            first_images_str = format_number(first_count, total_count, COLUMN_WIDTHS['first_images'], "percentage")
            wikidata_str = format_number(wikidata_count, total_count, COLUMN_WIDTHS['wikidata'], "percentage")
            
            print(f"{i:<{COLUMN_WIDTHS['index']}} {lang:<{COLUMN_WIDTHS['language']}} {total_count:<{COLUMN_WIDTHS['total_uses']}} {get_percentage(total_count, total_usages):<{COLUMN_WIDTHS['percent']}} " +
                  f"{first_images_str:<{COLUMN_WIDTHS['first_images']}} {wikidata_str:<{COLUMN_WIDTHS['wikidata']}}")
    
    duration = datetime.now() - start_time
    minutes, seconds = divmod(duration.seconds, 60)
    
    print(f"\n{HEADER_LINE}")
    print(f"✅ Analysis completed in {minutes} minutes and {seconds} seconds")
    print(HEADER_LINE)

def main() -> List[Dict]:
    """Run image usage analysis and return results."""
    try:
        args = parse_arguments()
        print("=== Starting Analysis ===")
        start_time = datetime.now()
        
        all_files = get_commons_category_files(args.category, args.depth)
        if not all_files:
            print("No files found in category. Exiting.")
            sys.exit(1)
        
        stats = initialize_statistics()
        results = process_files(all_files, args, stats)
        
        print_detailed_results(results)
        print_summary_statistics(results, stats, args.category, start_time, len(all_files))
        
        return results
        
    except KeyboardInterrupt:
        print("\nScript interrupted by user")
        sys.exit(1)
    except WikiAPIError as e:
        print(f"\nAPI Error: {str(e)}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"\nUnexpected error: {str(e)}", file=sys.stderr)
        sys.exit(1)

def parse_arguments() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Analyze how and when images from a Commons category are used across all Wikipedias."
    )
    parser.add_argument(
        "--category",
        required=True,
        help="Name of the Commons category (without 'Category:' prefix)"
    )
    parser.add_argument(
        "--depth",
        type=int,
        default=0,
        help="Recursion depth for subcategories (0 = no recursion, 1 = direct subcategories only, etc.)"
    )
    parser.add_argument(
        "--limit-wikis",
        type=int,
        help="Limit analysis to the top N Wikipedias by usage count"
    )
    parser.add_argument(
        "--skip-wikidata",
        action="store_true",
        help="Skip checking Wikidata for images not found in wikitext"
    )
    return parser.parse_args()

def initialize_statistics() -> StatisticsTracker:
    """Initialize statistics tracking."""
    return StatisticsTracker()

def process_files(all_files: List[str], args: argparse.Namespace, stats: StatisticsTracker) -> List[Dict]:
    """Process each file in the category"""
    results = []
    
    for file_title in all_files:
        print(f"== Processing: {file_title} ==")
        wiki_pages = get_global_usage_of_file(file_title)
        
        if not wiki_pages:
            print(f"No Wikipedia usage found.\n")
            continue
        
        stats.files_used_on_wikipedias += 1
        
        for wiki, pages in wiki_pages.items():
            stats.usage_by_wiki[wiki] += len(pages)
        
        # If limiting wikis via flag, get the top N by usage count
        if args.limit_wikis and len(wiki_pages) > args.limit_wikis:
            top_wikis = sorted(stats.usage_by_wiki.items(), key=lambda x: x[1], reverse=True)[:args.limit_wikis]
            top_wiki_domains = [wiki for wiki, _ in top_wikis]
            wiki_pages = {wiki: pages for wiki, pages in wiki_pages.items() if wiki in top_wiki_domains}
            print(f"Limiting analysis to top {args.limit_wikis} wikis: {', '.join(top_wiki_domains)}")
        
        for wiki, pages in wiki_pages.items():
            process_wiki_pages(wiki, pages, file_title, args, stats, results)
    
    return results

def process_wiki_pages(wiki: str, pages: List[str], file_title: str, args: argparse.Namespace, 
                      stats: StatisticsTracker, results: List[Dict]) -> None:
    """Process pages for a specific wiki.
    
    Args:
        wiki: The wiki domain
        pages: List of page titles to process
        file_title: The file being processed
        args: Command line arguments
        stats: StatisticsTracker instance
        results: List to append results to
    """
    lang_code = get_language_code(wiki)
    print(f"Processing {len(pages)} pages on {wiki} ({lang_code})...")
    
    for page_title in pages:
        stats.wiki_pages_seen[wiki].add(page_title)
        print(f"Analyzing usage on {wiki} page '{page_title}'...")
        
        try:
            if args.skip_wikidata:
                info = find_earliest_wikitext_introduction(wiki, page_title, file_title)
            else:
                info = find_earliest_introduction(wiki, page_title, file_title)
            
            if info:
                update_statistics(info, stats, file_title, wiki, lang_code)
                results.append(create_result_entry(info, file_title, wiki, lang_code, page_title))
                
        except Exception as e:
            print(f"Error processing {file_title} on {wiki} page '{page_title}': {e}")
            raise
            
        print()

def update_statistics(info: Dict, stats: StatisticsTracker, file_title: str, wiki: str, lang_code: str) -> None:
    """Update statistics based on the file usage info.
    
    Args:
        info: Dictionary containing file usage information
        stats: StatisticsTracker instance
        file_title: The file being processed
        wiki: The wiki domain
        lang_code: The language code
    """
    stats.total_usages_by_file[file_title] += 1
    stats.total_usages_by_wiki[wiki] += 1
    stats.total_usages_by_lang[lang_code] += 1
    
    if info["first_image"]:
        stats.first_image_by_file[file_title] += 1
        stats.first_image_by_wiki[wiki] += 1
        stats.first_image_by_lang[lang_code] += 1
    
    if info.get("from_wikidata", False):
        stats.wikidata_sourced_count += 1
        stats.wikidata_sourced_by_wiki[wiki] += 1
        
        if info.get("first_p18", False):
            stats.first_p18_count += 1
        else:
            stats.replaced_p18_count += 1

def create_result_entry(info: Dict, file_title: str, wiki: str, lang_code: str, page_title: str) -> Dict:
    """Create a result entry for a file usage.
    
    Args:
        info: Dictionary containing file usage information
        file_title: The file being processed
        wiki: The wiki domain
        lang_code: The language code
        page_title: The page title
        
    Returns:
        Dictionary containing the formatted result entry
    """
    return {
        "file": file_title,
        "wiki": wiki,
        "language": lang_code,
        "page": page_title,
        "introduced_revision_id": info["introduced_revision_id"],
        "introduced_timestamp": info["introduced_timestamp"] if info.get("timestamp_is_formatted", False) else format_timestamp(info["introduced_timestamp"]),
        "first_image": info["first_image"],
        "from_wikidata": info.get("from_wikidata", False),
        "wikidata_item": info.get("wikidata_item", None),
        "first_p18": info.get("first_p18", None),
        "previous_p18_images": info.get("previous_p18_images", [])
    }

def print_detailed_results(results: List[Dict]) -> None:
    """Print detailed results for each file usage."""
    print("\n=== DETAILED RESULTS ===")
    
    for i, r in enumerate(results):
        print(f"\n[{i+1}/{len(results)}] {r['file']}")
        print("─" * min(80, len(r['file']) + 7))
        
        print(f"📄 Page:        {r['page']} on {r['wiki']} ({r['language']})")
        
        timestamp_display = r['introduced_timestamp']
        
        if r.get("from_wikidata", False):
            wikidata_item = r.get("wikidata_item", "Unknown")
            wikidata_link = f"https://www.wikidata.org/wiki/{wikidata_item}"
            
            print(f"🔄 Source:      Via Wikidata ({wikidata_item}) - {wikidata_link}")
            print(f"⏱️ Introduced:  {timestamp_display}")
            
            if r.get("first_p18", False):
                print(f"ℹ️ P18 Status:  First P18 value for this item")
            else:
                prev_count = len(r.get("previous_p18_images", []))
                print(f"ℹ️ P18 Status:  Replaced {prev_count} previous P18 value(s)")
                
                if prev_count > 0:
                    prev_images = r.get("previous_p18_images", [])
                    if len(prev_images) > 0:
                        print(f"📜 P18 History:")
                        for idx, img in enumerate(prev_images[:MAX_PREV_IMAGES]):
                            print(f"   {idx+1}. {img}")
                        if len(prev_images) > MAX_PREV_IMAGES:
                            print(f"   ... and {len(prev_images) - MAX_PREV_IMAGES} more previous values")
        else:
            print(f"🔄 Source:      Directly in Wikipedia wikitext")
            print(f"⏱️ Introduced:  {timestamp_display} (revision {r['introduced_revision_id']})")
        
        print(f"🥇 First image: {'Yes' if r['first_image'] else 'No'}")

if __name__ == "__main__":
    main()
