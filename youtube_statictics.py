import requests
import json
from datetime import datetime
from collections import Counter
import re
import pandas as pd


# Define a class to handle YouTube API requests and data processing
class YTRequestProcessor:
    def __init__(self, api_key, results):
        self.api_key = api_key
        self.results = results

    def get_video_data(self):
        """
        Fetch video data from the YouTube API.

        This function retrieves video data from the YouTube API based on the specified parameters.
        It fetches information such as snippet, content details, statistics, player, topic details,
        live streaming details, and more.

        Returns:
            list: A list of video items containing various information retrieved from the API.
        """
        video_url = f'https://www.googleapis.com/youtube/v3/videos?part=snippet%2CcontentDetails%2Cstatistics%2Cplayer%2CtopicDetails%2CliveStreamingDetails&chart=mostPopular&maxResults={self.results}&key={self.api_key}'
        json_url = requests.get(video_url)
        data = json.loads(json_url.text)
        return data.get("items", [])

    # Fetch channel data from YouTube API
    def get_channel_data(self, channel_id):
        channel_url = f"https://www.googleapis.com/youtube/v3/channels?part=statistics&id={channel_id}&key={self.api_key}"
        json_url_channel = requests.get(channel_url)
        data_channel = json.loads(json_url_channel.text)
        return data_channel.get("items", [])[0]["statistics"]


# Define a class to transform and process data
class DataTransformer:
    # Convert a list of strings to datetime objects
    @staticmethod
    def string_to_date(input):
        """
        Convert a list of string dates to datetime objects.

        Args:
            input (list): A list of string dates in the format '%Y-%m-%dT%H:%M:%SZ'.

        Returns:
            list: A list of datetime objects corresponding to the input dates.
        """
        results = []
        for data in input:
            datetime_obj = datetime.datetime.strptime(data, '%Y-%m-%dT%H:%M:%SZ')
            results.append(datetime_obj)
        return results

    @staticmethod
    def remove_special_characters(sentence):
        """
        Remove special characters from a sentence.

        Args:
            sentence (str): The input sentence containing special characters.

        Returns:
            str: The cleaned sentence with special characters removed.
        """
        cleaned_sentence = re.sub(r'[^a-zA-Z0-9\s]', '', sentence)
        return cleaned_sentence

    @staticmethod
    def get_most_frequent_word_in_tags(records):
        """
        Get the most frequent word in a list of tags.

        Args:
            records (list): A list of tag lists.

        Returns:
            list: A list of the most frequent words from the input tag lists.
        """
        results = []
        for tags_list in records:
            if tags_list != 'Unknown':
                all_text = " ".join(tags_list)
                words = re.findall(r'\b(?![0-9]+\b)\w+\b', all_text.lower())
                word_counts = Counter(words)
                most_common_word, count = word_counts.most_common(1)[0]
                results.append(most_common_word)
            else:
                results.append('Unknown')
        return results

    @staticmethod
    def count_ratio(values, channel_values):
        """
        Calculate ratios between two sets of values, handling ZeroDivisionError.

        Args:
            values (list): List of values for the numerator.
            channel_values (list): List of values for the denominator (channels).

        Returns:
            list: List of ratios between corresponding values. If denominator is zero, ratio is set to 0.
        """
        result = []
        for value, channel_value in zip(values, channel_values):
            try:
                ratio = round(value / channel_value, 6)
                result.append(ratio)
            except ZeroDivisionError:
                result.append(0)
        return result

    @staticmethod
    def categorize_duration(duration_values):
        """
        Categorize durations into "short", "medium", or "long".

        Args:
            duration_values (list): List of duration values in seconds.

        Returns:
            list: List of category labels ("short", "medium", or "long") based on duration values.
        """
        short_threshold = 180  # 3 minutes (180 seconds)
        medium_threshold = 600  # 10 minutes (600 seconds)
        duration_categories = []
        for duration in duration_values:
            if duration < short_threshold:
                category = "short"
            elif duration < medium_threshold:
                category = "medium"
            else:
                category = "long"
            duration_categories.append(category)
        return duration_categories

    @staticmethod
    def convert_string_to_datetime(timestamp_str):
        """
        Convert string timestamps to datetime objects.

        Args:
            timestamp_str (list): List of string timestamps in the format "%Y-%m-%dT%H:%M:%SZ".

        Returns:
            list: List of datetime objects corresponding to the input string timestamps.
        """
        results = []
        for times in timestamp_str:
            timestamp_format = "%Y-%m-%dT%H:%M:%SZ"
            timestamp_dt = datetime.strptime(times, timestamp_format)
            results.append(timestamp_dt)
        return results

    @staticmethod
    def duration_transform(duration_list):
        """
        Transform duration strings to total seconds.

        Args:
            duration_list (list): List of duration strings in ISO8601 format.

        Returns:
            list: List of total seconds corresponding to the input duration strings.
        """
        result = []
        for duration in duration_list:
            if not duration:
                result.append(0)
                continue

            total_seconds = 0

            if 'H' in duration:
                hours = int(duration.split("H")[1].split("M")[0])
                total_seconds += hours * 3600

            if 'M' in duration:
                minutes = int(duration.split("M")[0].split("H" if 'H' in duration else "T")[1])
                total_seconds += minutes * 60

            if 'S' in duration:
                seconds = int(duration.split("S")[0].split("M" if 'M' in duration else "T")[1])
                total_seconds += seconds

            result.append(total_seconds)
        return result

    # Map category IDs to category names
    @staticmethod
    def get_category_name(category_id):
        categories = {
            '1': 'Film & Animation',
            '2': 'Autos & Vehicles',
            '10': 'Music',
            '15': 'Pets & Animals',
            '17': 'Sports',
            '18': 'Short Movies',
            '19': 'Travel & Events',
            '20': 'Gaming',
            '21': 'Videoblogging',
            '22': 'People & Blogs',
            '23': 'Comedy',
            '24': 'Entertainment',
            '25': 'News & Politics',
            '26': 'Howto & Style',
            '27': 'Education',
            '28': 'Science & Technology',
            '29': 'Nonprofits & Activism',
            '30': 'Movies',
            '31': 'Anime/Animation',
            '32': 'Action/Adventure',
            '33': 'Classics',
            '34': 'Comedy',
            '35': 'Documentary',
            '36': 'Drama',
            '37': 'Family',
            '38': 'Foreign',
            '39': 'Horror',
            '40': 'Sci-Fi/Fantasy',
            '41': 'Thriller',
            '42': 'Shorts',
            '43': 'Shows',
            '44': 'Trailers',
        }
        if isinstance(category_id, list):
            return [categories.get(cid, 'Unknown') for cid in category_id]
        else:
            return categories.get(category_id, 'Unknown')


# Define a class to analyze YouTube video statistics
class YTStatsAnalyzer:
    """
    Initialize the YTStatsAnalyzer.

    Args:
        api_key (str): Your YouTube Data API key.
        results (int): Maximum number of results to retrieve.
    """
    def __init__(self, api_key, results):

        self.request_processor = YTRequestProcessor(api_key, results)
        self.data_transformer = DataTransformer()

    # Analyze video statistics and return a DataFrame
    def analyze_statistics(self):
        """
        Analyze video statistics and return a DataFrame containing extracted data.

        Returns:
            pandas.DataFrame: DataFrame containing analyzed video statistics.
        """
        video_data = self.request_processor.get_video_data()

        # Initialize lists to store extracted data
        video_id = []
        watch = []
        video_title = []
        description = []
        channel_title = []
        published_at = []
        duration = []
        tags = []
        category_id = []
        view_count = []
        like_count = []
        comment_count = []
        channel_id = []
        channel_view_count = []
        channel_sub_count = []
        channel_video_count = []
        picture_url = []
        language = []

        # Loop through video data and extract relevant information
        for item in video_data:
            video_id.append(item['id'])
            video_title.append(item['snippet']['title'])
            watch.append("https://www.youtube.com/watch?v=" + item['id'])
            description.append(item['snippet']['description'])
            channel_title.append(item['snippet']['channelTitle'])
            published_at.append(item['snippet']['publishedAt'])
            duration.append(item['contentDetails']['duration'])
            tags.append(item['snippet'].get('tags', 'Unknown'))
            category_id.append(item['snippet']['categoryId'])
            view_count.append(int(item['statistics'].get('viewCount', 0)))
            like_count.append(int(item['statistics'].get('likeCount', 0)))
            comment_count.append(int(item['statistics'].get('commentCount', 0)))
            channel_id.append(item['snippet']['channelId'])
            picture_url.append(item['snippet']['thumbnails']['standard']['url'])
            language.append(item['snippet'].get('defaultAudioLanguage', 'Unknown'))

            # Fetch channel statistics using the request processor
            channel_statistics = self.request_processor.get_channel_data(item['snippet']['channelId'])
            channel_view_count.append(int(channel_statistics.get('viewCount', 0)))
            channel_sub_count.append(int(channel_statistics.get('subscriberCount', 0)))
            channel_video_count.append(int(channel_statistics.get('videoCount', 0)))

        # Convert video durations to total seconds
        duration_sec = self.data_transformer.duration_transform(duration)

        # Create a dictionary to hold the extracted data
        data = {
            "video_id": video_id,
            "video_title": video_title,
            "channel_id": channel_id,
            "channel_title": channel_title,
            "date_of_published": self.data_transformer.convert_string_to_datetime(published_at),
            "most_frequent_word": self.data_transformer.get_most_frequent_word_in_tags(tags),
            "category": self.data_transformer.get_category_name(category_id),
            "view_count": view_count,
            "like_count": like_count,
            "view_count_ratio": self.data_transformer.count_ratio(view_count, channel_view_count),
            "like_count_ratio": self.data_transformer.count_ratio(like_count, channel_sub_count),
            "comment_count": comment_count,
            "duration_sec": duration_sec,
            "categorize_duration": self.data_transformer.categorize_duration(duration_sec),
            "channel_view_count": channel_view_count,
            "channel_sub_count": channel_sub_count,
            "channel_video_count": channel_video_count,
            "watch_video": watch,
            "picture": picture_url,
            "language": language
        }

        # Create a DataFrame from the extracted data
        return pd.DataFrame(data)
