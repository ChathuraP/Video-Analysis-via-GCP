import time
import os

from google.cloud import videointelligence as vi
from typing import Optional, Sequence, cast
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from google.cloud.exceptions import GoogleCloudError

OUTPUT_BUCKET = "gs://outputbucket-cloud9"
PROJECT_ID = "video-analyzer-407616"
DATASET_ID = "video_analytics"
TABLE_ID_LABELS = "annotation_labels"
TABLE_ID_TRANSCRIPT = "annotation_transcripts"

features = [
    vi.Feature.OBJECT_TRACKING,
    vi.Feature.LABEL_DETECTION,
    vi.Feature.SHOT_CHANGE_DETECTION,
    vi.Feature.SPEECH_TRANSCRIPTION,
    vi.Feature.LOGO_RECOGNITION,
    vi.Feature.EXPLICIT_CONTENT_DETECTION,
    vi.Feature.TEXT_DETECTION,
    vi.Feature.FACE_DETECTION,
    vi.Feature.PERSON_DETECTION,
]

speech_config = vi.SpeechTranscriptionConfig(
    language_code="en-US",
    enable_automatic_punctuation=True,
)

person_config = vi.PersonDetectionConfig(
    include_bounding_boxes=True,
    include_attributes=False,
    include_pose_landmarks=True,
)

face_config = vi.FaceDetectionConfig(
    include_bounding_boxes=True,
    include_attributes=True,
)

video_context = vi.VideoContext(
    speech_transcription_config=speech_config,
    person_detection_config=person_config,
    face_detection_config=face_config,
)


def analyze_video(event, context):
    print(event)
    input_uri = "gs://" + event["bucket"] + "/" + event["name"]
    file_stem = event["name"].split(".")[0]
    output_uri = f"{OUTPUT_BUCKET}/{file_stem}.json"
    request = {
        "features": features,
        "input_uri": input_uri,
        "output_uri": output_uri,
        "video_context": video_context,
    }
    print(f'Processing video "{input_uri}"...')
    video_client = vi.VideoIntelligenceServiceClient()
    operation = video_client.annotate_video(request)
    response = cast(vi.AnnotateVideoResponse, operation.result())
    results = response.annotation_results
    return results

def sorted_by_first_segment_confidence(labels: Sequence[vi.LabelAnnotation],) -> Sequence[vi.LabelAnnotation]:
    return sorted(labels, key=lambda label: label.segments[0].confidence, reverse=True)

def category_entities_to_str(category_entities: Sequence[vi.Entity]) -> str:
    if not category_entities:
        return ""
    entities = ", ".join([e.description for e in category_entities])
    return f" ({entities})"

def print_video_labels(results: vi.VideoAnnotationResults):
    labels = sorted_by_first_segment_confidence(results.segment_label_annotations)
    print(f" Video labels: {len(labels)} ".center(80, "-"))
    for label in labels:
        categories = category_entities_to_str(label.category_entities)
        for segment in label.segments:
            confidence = segment.confidence
            t1 = segment.segment.start_time_offset.total_seconds()
            t2 = segment.segment.end_time_offset.total_seconds()
            print(f"{confidence:4.0%} | {t1:7.3f} | {t2:7.3f} | {label.entity.description}{categories}")

def print_video_speech(results: vi.VideoAnnotationResults, min_confidence: float = 0.8):
    def keep_transcription(transcription: vi.SpeechTranscription) -> bool:
        return min_confidence <= transcription.alternatives[0].confidence

    transcriptions = results.speech_transcriptions
    transcriptions = [t for t in transcriptions if keep_transcription(t)]

    print(f" Speech transcriptions: {len(transcriptions)} ".center(80, "-"))
    for transcription in transcriptions:
        first_alternative = transcription.alternatives[0]
        confidence = first_alternative.confidence
        transcript = first_alternative.transcript
        print(f" {confidence:4.0%} | {transcript.strip()}")

def create_bigquery_tables():
    client = bigquery.Client()
    # Define schema for the first table
    schema_labels = [
        bigquery.SchemaField("file_name", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("label", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("confidence", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("start_time", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("end_time", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("file_uri", "STRING", mode="REQUIRED"),
    ]
    # Define schema for the second table
    schema_transcript = [
        bigquery.SchemaField("file_name", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("transcript", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("confidence", "FLOAT", mode="NULLABLE"),
    ]

    # Create the labels table
    table_ref_labels = client.dataset(DATASET_ID).table(TABLE_ID_LABELS)
    try:
        client.get_table(table_ref_labels)
        print("Table {} already exists. Skipping creation.".format(table_ref_labels.table_id))
    except NotFound:
        table_labels = bigquery.Table(table_ref_labels, schema=schema_labels)
        table_labels = client.create_table(table_labels)
        print("Table {} created.".format(table_labels.table_id))

    # Create the transcript table
    table_ref_transcript = client.dataset(DATASET_ID).table(TABLE_ID_TRANSCRIPT)
    try:
        client.get_table(table_ref_transcript)
        print("Table {} already exists. Skipping creation.".format(table_ref_transcript.table_id))
    except NotFound:
        table_transcript = bigquery.Table(table_ref_transcript, schema=schema_transcript)
        table_transcript = client.create_table(table_transcript)
        print("Table {} created.".format(table_transcript.table_id))

def store_results_in_bigquery(video_uri, results):
    results_labels = results[0]
    results_transcript = results[1]
    url_parts = video_uri.split('/')
    file_name = url_parts[-1]

    client = bigquery.Client()
    rows_to_insert_labels = []
    rows_to_insert_transcript = []

    # Prepare data into the 'labels' table
    for label_annotation in results_labels.segment_label_annotations:
        for segment in label_annotation.segments:
            start_time = segment.segment.start_time_offset.total_seconds()
            end_time = segment.segment.end_time_offset.total_seconds()
            confidence = segment.confidence
            rows_to_insert_labels.append({
                "file_name": file_name,
                "label": label_annotation.entity.description,
                "confidence": confidence,
                "start_time": start_time,
                "end_time": end_time,
                "file_uri": video_uri,
            })

    # Prepare data into the 'transcript' table
    transcriptions = results_transcript.speech_transcriptions
    for transcription in transcriptions:
        first_alternative = transcription.alternatives[0]
        confidence = first_alternative.confidence
        transcript = first_alternative.transcript
        rows_to_insert_transcript.append({
            "file_name": file_name,
            "transcript": transcript,
            "confidence": confidence,
        })

    try:
        # Insert into the 'labels' table
        table_labels = client.dataset(DATASET_ID).table(TABLE_ID_LABELS)
        annotation_labels = client.insert_rows_json(table_labels, rows_to_insert_labels)
        if annotation_labels == []:
            print("New rows have been added to the 'labels' table.")
        else:
            print("Encountered errors while inserting rows into 'labels' table: {}".format(annotation_labels))

        # Insert into the 'transcript' table
        table_transcript = client.dataset(DATASET_ID).table(TABLE_ID_TRANSCRIPT)
        annotation_transcripts = client.insert_rows_json(table_transcript, rows_to_insert_transcript)
        if annotation_transcripts == []:
            print("New rows have been added to the 'transcript' table.")
        else:
            print("Encountered errors while inserting rows into 'transcript' table: {}".format(annotation_transcripts))
    except GoogleCloudError as e:
        print("Error inserting rows into BigQuery: {}".format(e))


def process_video(event, context):
    input_uri = "gs://" + event["bucket"] + "/" + event["name"]
    results = analyze_video(event, context)
    print_video_labels(results[0])
    print_video_speech(results[1])
    create_bigquery_tables()
    store_results_in_bigquery(input_uri, results)
