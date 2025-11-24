"""
Data Transformer Module
=======================
Transform TikTok JSON data into normalized staging tables format.

This module extracts and transforms data from TikTok crawler JSON
into three normalized tables: Authors, Videos, and VideoInteractions.

Author: LoaderStaging Team
Created: 2025-11-24
"""

import logging
from typing import Dict, Any, List, Optional
from datetime import datetime

logger = logging.getLogger(__name__)


# ============================================================================
# Constants
# ============================================================================

DEFAULT_AUTHOR_NAME = ""
DEFAULT_AVATAR = ""
DEFAULT_TEXT_CONTENT = ""
DEFAULT_DURATION = 0
DEFAULT_COUNT = 0


class TikTokTransformer:
    """
    Class TikTokTransformer: Transform JSON flat thành 3 bảng normalized
    
    Nhiệm vụ:
    - Extract author info từ authorMeta
    - Extract video info từ video fields
    - Extract interaction stats từ count fields
    - Transform format (timestamp → datetime string)
    - Handle missing fields với default values
    """
    
    @staticmethod
    def extract_author(item: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extract thông tin author từ JSON item
        
        Args:
            item: JSON object chứa authorMeta
            
        Returns:
            Dict chứa author info: {
                'author_id': str,
                'author_name': str,
                'avatar': str
            }
        """
        try:
            author_meta = item.get('authorMeta', {})
            
            author = {
                'author_id': str(author_meta.get('id', '')),
                'author_name': author_meta.get('name', ''),
                'avatar': author_meta.get('avatar', '')
            }
            
            # Validate required fields
            if not author['author_id']:
                logger.warning(f"Missing author_id in item: {item.get('id')}")
                return None
                
            return author
            
        except Exception as e:
            logger.error(f"Error extracting author: {e}")
            return None
    
    @staticmethod
    def extract_video(item: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extract thông tin video từ JSON item
        
        Args:
            item: JSON object chứa video info
            
        Returns:
            Dict chứa video info: {
                'video_id': str,
                'author_id': str,
                'text_content': str,
                'duration': int,
                'create_time': str (YYYY-MM-DD HH:MM:SS),
                'web_video_url': str
            }
        """
        try:
            author_meta = item.get('authorMeta', {})
            video_meta = item.get('videoMeta', {})
            
            # Convert createTime (Unix timestamp) to datetime string
            create_time = None
            if 'createTime' in item:
                try:
                    timestamp = int(item['createTime'])
                    create_time = datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')
                except (ValueError, TypeError) as e:
                    logger.warning(f"Invalid createTime for video {item.get('id')}: {e}")
            
            video = {
                'video_id': str(item.get('id', '')),
                'author_id': str(author_meta.get('id', '')),
                'text_content': item.get('text', ''),
                'duration': video_meta.get('duration', 0),
                'create_time': create_time,
                'web_video_url': item.get('webVideoUrl', '')
            }
            
            # Validate required fields
            if not video['video_id']:
                logger.warning(f"Missing video_id in item")
                return None
            if not video['author_id']:
                logger.warning(f"Missing author_id for video {video['video_id']}")
                return None
                
            return video
            
        except Exception as e:
            logger.error(f"Error extracting video: {e}")
            return None
    
    @staticmethod
    def extract_interaction(item: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extract thông tin interaction stats từ JSON item
        
        Args:
            item: JSON object chứa interaction counts
            
        Returns:
            Dict chứa interaction stats: {
                'video_id': str,
                'digg_count': int,
                'play_count': int,
                'share_count': int,
                'comment_count': int,
                'collect_count': int
            }
        """
        try:
            interaction = {
                'video_id': str(item.get('id', '')),
                'digg_count': int(item.get('diggCount', 0)),
                'play_count': int(item.get('playCount', 0)),
                'share_count': int(item.get('shareCount', 0)),
                'comment_count': int(item.get('commentCount', 0)),
                'collect_count': int(item.get('collectCount', 0))
            }
            
            # Validate required fields
            if not interaction['video_id']:
                logger.warning(f"Missing video_id in interaction")
                return None
                
            return interaction
            
        except Exception as e:
            logger.error(f"Error extracting interaction: {e}")
            return None
    
    @staticmethod
    def transform_batch(json_data: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
        """
        Transform toàn bộ JSON array thành 3 lists: authors, videos, interactions
        
        Args:
            json_data: List of JSON objects từ TikTok crawler
            
        Returns:
            Dict chứa 3 lists:
            {
                'authors': List[Dict],
                'videos': List[Dict],
                'interactions': List[Dict]
            }
            
        Example:
            >>> json_data = [{"id": "123", "authorMeta": {...}, ...}, ...]
            >>> result = TikTokTransformer.transform_batch(json_data)
            >>> result['authors']  # List of author dicts
            >>> result['videos']   # List of video dicts
            >>> result['interactions']  # List of interaction dicts
        """
        authors = []
        videos = []
        interactions = []
        
        author_ids_seen = set()
        video_ids_seen = set()
        
        for item in json_data:
            try:
                # Extract author (dedup by author_id)
                author = TikTokTransformer.extract_author(item)
                if author and author['author_id'] not in author_ids_seen:
                    authors.append(author)
                    author_ids_seen.add(author['author_id'])
                
                # Extract video
                video = TikTokTransformer.extract_video(item)
                if video and video['video_id'] not in video_ids_seen:
                    videos.append(video)
                    video_ids_seen.add(video['video_id'])
                
                # Extract interaction
                interaction = TikTokTransformer.extract_interaction(item)
                if interaction:
                    interactions.append(interaction)
                    
            except Exception as e:
                logger.error(f"Error transforming item {item.get('id')}: {e}")
                continue
        
        logger.info(f"Transformed: {len(authors)} authors, {len(videos)} videos, {len(interactions)} interactions")
        
        return {
            'authors': authors,
            'videos': videos,
            'interactions': interactions
        }


# ============================================================================
# Transform CLI - Standalone Transformer
# ============================================================================

def main():
    """
    CLI để test transformer standalone
    
    Usage:
        python transformer.py input.json output.json
    """
    import sys
    import json
    from pathlib import Path
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    if len(sys.argv) < 2:
        print("Usage: python transformer.py <input_json_file> [output_json_file]")
        print("\nExample:")
        print("  python transformer.py data.json")
        print("  python transformer.py data.json transformed.json")
        sys.exit(1)
    
    input_file = Path(sys.argv[1])
    output_file = Path(sys.argv[2]) if len(sys.argv) > 2 else None
    
    if not input_file.exists():
        logger.error(f"Input file not found: {input_file}")
        sys.exit(1)
    
    try:
        # Load JSON
        logger.info(f"Loading JSON from {input_file}...")
        with open(input_file, 'r', encoding='utf-8') as f:
            json_data = json.load(f)
        
        logger.info(f"Loaded {len(json_data)} items")
        
        # Transform
        logger.info("Transforming data...")
        result = TikTokTransformer.transform_batch(json_data)
        
        # Print summary
        print("\n" + "=" * 80)
        print("TRANSFORMATION SUMMARY")
        print("=" * 80)
        print(f"Authors:      {len(result['authors'])}")
        print(f"Videos:       {len(result['videos'])}")
        print(f"Interactions: {len(result['interactions'])}")
        print("=" * 80)
        
        # Save output if specified
        if output_file:
            logger.info(f"Saving transformed data to {output_file}...")
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(result, f, indent=2, ensure_ascii=False)
            logger.info(f"✓ Saved to {output_file}")
        else:
            # Print sample
            print("\nSample Authors (first 3):")
            for author in result['authors'][:3]:
                print(f"  - {author['author_id']}: {author['author_name']}")
            
            print("\nSample Videos (first 3):")
            for video in result['videos'][:3]:
                print(f"  - {video['video_id']}: {video['text_content'][:50]}...")
            
            print("\nSample Interactions (first 3):")
            for interaction in result['interactions'][:3]:
                print(f"  - Video {interaction['video_id']}: "
                      f"{interaction['play_count']} views, "
                      f"{interaction['digg_count']} likes")
        
        logger.info("✓ Transformation completed successfully")
        
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON file: {e}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Transformation failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()
