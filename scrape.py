from atproto import FirehoseSubscribeReposClient, parse_subscribe_repos_message, CAR, IdResolver, DidInMemoryCache
import json
import time

class FirehoseScraper:
    def __init__(self, output_file="bluesky_posts.jsonl"):
        self.client = FirehoseSubscribeReposClient()
        self.output_file = output_file
        self.post_count = 0
        self.start_time = None
        self.cache = DidInMemoryCache() 
        self.resolver = IdResolver(cache=self.cache)
        
        
    def process_message(self, message) -> None:
        """Process a single message from the firehose"""
        try:
            commit = parse_subscribe_repos_message(message)
            if not hasattr(commit, 'ops'):
                return

            for op in commit.ops:
                if op.action == 'create' and op.path.startswith('app.bsky.feed.post/'):
                    self._process_post(commit, op)

        except Exception as e:
            print(f"Error processing message: {e}")

    def _process_post(self, commit, op):
        """Process a single post operation"""
        try:
            author_handle = self._resolve_author_handle(commit.repo)
            car = CAR.from_bytes(commit.blocks)
            for record in car.blocks.values():
                if isinstance(record, dict) and record.get('$type') == 'app.bsky.feed.post':
                    post_data = self._extract_post_data(record, commit.repo, op.path, author_handle)
                    self._save_post_data(post_data)
        except Exception as e:
            print(f"Error processing record: {e}")

    def _resolve_author_handle(self, repo):
        """Resolve the author handle from the DID"""
        try:
            resolved_info = self.resolver.did.resolve(repo)
            return resolved_info.also_known_as[0].split('at://')[1] if resolved_info.also_known_as else repo
        except Exception as e:
            print(f"Could not resolve handle for {repo}: {e}")
            return repo  # Fallback to DID

    def _extract_post_data(self, record, repo, path, author_handle):
        """Extract post data from a record"""
        has_images = self._check_for_images(record)
        reply_to = self._get_reply_to(record)
        return {
            'text': record.get('text', ''),
            'created_at': record.get('createdAt', ''),
            'author': author_handle,
            'uri': f'at://{repo}/{path}',
            'has_images': has_images,
            'reply_to': reply_to
        }

    def _check_for_images(self, record):
        """Check if the post has images"""
        embed = record.get('embed', {})
        return (
            embed.get('$type') == 'app.bsky.embed.images' or
            (embed.get('$type') == 'app.bsky.embed.external' and 'thumb' in embed)
        )

    def _get_reply_to(self, record):
        """Get the URI of the post being replied to"""
        reply_ref = record.get('reply', {})
        return reply_ref.get('parent', {}).get('uri')

    def _save_post_data(self, post_data):
        """Save post data to the output file"""
        with open(self.output_file, 'a') as f:
            json.dump(post_data, f)
            f.write('\n')
        self.post_count += 1
        # print(f"Saved post by @{post_data['author']}: {post_data['text'][:50]}...")

    def start_collection(self, duration_seconds=30):
        """Start collecting posts for the specified duration"""
        print(f"Starting collection for {duration_seconds} seconds...")
        self.start_time = time.time()
        end_time = self.start_time + duration_seconds

        def message_handler(message):
            if time.time() > end_time:
                self._stop_collection()
            else:
                self.process_message(message)

        try:
            self.client.start(message_handler)
        except KeyboardInterrupt:
            print("\nCollection stopped by user.")
            self._stop_collection()

    def _stop_collection(self):
        """Stop the collection and print summary"""
        elapsed = time.time() - self.start_time
        rate = self.post_count / elapsed if elapsed > 0 else 0
        print(f"\nCollection complete!")
        print(f"Collected {self.post_count} posts in {elapsed:.2f} seconds")
        print(f"Average rate: {rate:.1f} posts/sec")
        print(f"Output saved to: {self.output_file}")
        self.client.stop()

if __name__ == "__main__":
    archiver = FirehoseScraper()
    archiver.start_collection(duration_seconds=30)
