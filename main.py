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
        # audio_converter.walk_mp3_folders("D:/CHAMLIVE")
        p = Path("D:/CHAMLIVE")
        root_node = audio_converter.build_tree(p)
        tree_list = {}

        print("Extracting tree structure to CSV format...")
        tree_list = audio_converter.extract_tree(root_node)
        files = []
        folders = []    
        #print(f"{item['name']} (ID: {node_id}, Parent ID: {item['parent_id']}: {'File' if item['is_file'] else 'Folder'})")
        for node_id, item in tree_list.items():
            if node_id == 1:
                print(f"Root folder: {item['name']} (ID: {node_id}) {item['is_file']}")
            if item['is_file']:
                #row = f"{node_id}|{item['name']}| {item['parent_id']}"
                row = make_row_dict(node_id, item)
                files.append(row)
            else:
                row = f"{node_id}|{item['name']}| {item['parent_id']}|0|0|0|0|1|null"
                folders.append(row)

        #write_files("folders.csv", folders)
        # save files as a JSON file
        with open("files.json", "w", encoding="utf-8") as f:
            json.dump(files, f, ensure_ascii=False, indent=4)

        print("Finished extracting tree structure to JSON format.")
        print(f"Total Folders: {len(folders)}")
        print(f"Total Files: {len(files)}")

        #audio_converter.print_tree(root_node)
        # folders, files = audio_converter.print_tree_with_counts(root_node)
        # print(f"Folders: {folders}")
        # print(f"Files: {files}")
    else:
        parser.print_help()
        print('\n')
        print("Please provide an argument")

