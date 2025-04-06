import argparse
import sys
import json
from datetime import datetime
from imageusage import main as images_main


def save_results(results: list, category: str) -> str:
    """Save results to a JSON file with category and timestamp in the filename."""
    safe_category = category.replace(' ', '_').replace('/', '_')
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = f"results_{safe_category}_{timestamp}.json"
    
    try:
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(results, f, indent=2, ensure_ascii=False)
        print(f"Detailed results saved to {output_file}")
        return output_file
    except IOError as e:
        print(f"Error saving results to {output_file}: {str(e)}", file=sys.stderr)
        raise


def main():
    parser = argparse.ArgumentParser(
        description="WikiPortraits CLI - Analyze image usage across Wikipedias"
    )
    subparsers = parser.add_subparsers(dest='command')

    images_parser = subparsers.add_parser('image-usage', help='Analyze image usage')
    images_parser.add_argument(
        '--category',
        required=True,
        help="Name of the Commons category (without 'Category:' prefix)"
    )
    images_parser.add_argument(
        '--json',
        action='store_true',
        help="Save detailed results to a JSON file with a timestamp-based filename"
    )
    images_parser.add_argument(
        '--limit-wikis',
        type=int,
        help="Limit analysis to the top N Wikipedias by usage count"
    )
    images_parser.add_argument(
        '--skip-wikidata',
        action='store_true',
        help="Skip checking Wikidata for images not found in wikitext"
    )

    args = parser.parse_args()

    if args.command == 'image-usage':
        new_argv = [sys.argv[0]]
        if args.category:
            new_argv.extend(['--category', args.category])
        if args.limit_wikis:
            new_argv.extend(['--limit-wikis', str(args.limit_wikis)])
        if args.skip_wikidata:
            new_argv.append('--skip-wikidata')
        
        original_argv = sys.argv
        sys.argv = new_argv
        
        try:
            results = images_main()
            if args.json:
                save_results(results, args.category)
        finally:
            sys.argv = original_argv
    else:
        parser.print_help()


if __name__ == "__main__":
    main() 
