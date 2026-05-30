import re

media_pattern = re.compile(
    r'''[`"']?MEDIA:\s*(?P<path>`[^`\n]+`|"[^"\n]+"|'[^'\n]+'|(?:~/|/)\S+(?:[^\S\n]+\S+)*?\.(?:png|jpe?g|gif|webp|mp4|mov|avi|mkv|webm|ogg|opus|mp3|wav|m4a|flac|epub|pdf|zip|rar|7z|docx?|xlsx?|pptx?|txt|csv|apk|ipa)(?=[\s`"',;:)\]}]|$))[`"']?'''
)

test_strings = [
    "MEDIA:file",
    "MEDIA:附件发送",
    "MEDIA:/tmp/test.png",
    "MEDIA:'report.pdf'",
    "MEDIA:`/tmp/some file.txt`"
]

for s in test_strings:
    match = media_pattern.search(s)
    if match:
        print(f"MATCH: {s} -> {match.group('path')}")
    else:
        print(f"NO MATCH: {s}")
