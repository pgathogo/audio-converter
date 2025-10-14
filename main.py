import json
from pathlib import Path
import argparse
from audio_convert import AudioConverter

def get_config(config_file: str) -> dict:
    config = {}
    # Read ini file to get dbf_folder, audio_folder, output_folder and make a dictionary
    with open(config_file, "r") as f:
        config_data = f.read().split("\n")
        for line in config_data:
            if line.startswith("#") or line == "": # Skip comments and empty lines
                continue
            key, value = line.split("=") # Split the line into key and value
            print(f"{key}: {value}")
            config[key] = value

    return config

def write_files(filename: str, lines: list):
    with open(filename, "w", encoding="utf-8") as f:
        for line in lines:
            f.write(line + "\n")

def make_row_dict(node_id: int, item: dict) -> dict:
    row = {}
    row['Node_ID'] = node_id
    row['Name'] = item['name']
    row['Parent_ID'] = item['parent_id']
    row['filepath'] = item.get('filepath','')
    row['outputfilepath'] = item.get('outputfilepath','')
    return row

if __name__ == "__main__":
    config_ini = "config.ini"
    config = get_config(config_ini)
    audio_converter = AudioConverter(**config)

    parser = argparse.ArgumentParser(description="Convert and rename audio files")

    parser.add_argument("--c", action="store_true", help="Convert audio files")
    parser.add_argument("--p", action="store_true", help="process converted files")
    parser.add_argument("--r", action="store_true", help="Rename audio files")
    parser.add_argument("--m", action="store_true", help="Read MP3 folder")
    parser.add_argument("--w", action="store_true", help="Walks through MP3 folders")
    args = parser.parse_args()

    if args.c:
        audio_converter.convert()
    elif args.p:
        audio_converter.process_import_data()
    elif args.r:
        audio_converter.rename_converted_files()
    elif args.m:
        audio_converter.convert_mp3_to_ogg()
    elif args.w:
        audio_converter.prepare_files_for_conversion(audio_converter.chamgei_music_folder)
    else:
        parser.print_help()
        print('\n')
        print("Please provide an argument")

