import os


def get_filename_without_extension(file_path):
    filename = os.path.basename(file_path)
    return filename.replace(get_filename_extension(file_path), "")

def get_filename_extension(file_path):
    filename = os.path.basename(file_path)
    
    parts = os.path.splitext(filename)
    ext = ""
    while parts[1]:
        ext = f"{parts[1]}{ext}"
        parts = os.path.splitext(parts[0])

    return ext
