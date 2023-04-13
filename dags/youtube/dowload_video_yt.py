import pytube

link = "https://www.youtube.com/watch?v=LsMcjV3LJ1U"
yt=pytube.YouTube(link)
yt.streams.get_lowest_resolution().download()
print("dowload",link)