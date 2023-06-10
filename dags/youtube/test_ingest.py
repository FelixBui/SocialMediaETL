from data.raw.youtube.ingest_ytb import Ingest_YTB



def testing():
    import os
    video_url = 'https://www.youtube.com/shorts/0Vf1TpucUss'
    src_file_path = "/Users/macos/Downloads/0Vf1TpucUss.mp4"
    ytb_loader = Ingest_YTB('youtube')
    ytb_loader.execute(video_url, src_file_path)
