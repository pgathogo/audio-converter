import os
import hashlib
import json
from timeit import default_timer as timer
import datetime
from datetime import timedelta

from pathlib import Path, WindowsPath

from dbf_reader import get_data

from subprocess import PIPE, run

from mssql_data import MSSQLData, read_registry


class Node:
    def __init__(self, name, parent=None):
        self.name = name
        self.path = None
        self.children = {}
        self.parent = parent
        self.is_file = False
        self.is_dir = False

    def add_child(self, child_node):
        self.children[child_node.name] = child_node

    def __repr__(self):
        return f"Node(name='{self.name}', type='{'file' if self.is_file else 'dir'}')"


class TreeNode:
    Folder_ID_COUNTER = 1
    File_ID_COUNTER = 1
    def __init__(self, name, is_file=False, filepath="", parent=None):
        self.name = name
        self.is_file = is_file
        self.filepath = filepath
        self.parent = parent
        self.children = []
        self.node_id = TreeNode.File_ID_COUNTER
        TreeNode.File_ID_COUNTER += 1
        if is_file:
             self.node_id = TreeNode.File_ID_COUNTER
             TreeNode.File_ID_COUNTER += 1
        else:
            self.node_id = TreeNode.Folder_ID_COUNTER
            TreeNode.Folder_ID_COUNTER += 1

    def add_child(self, child_node):
        child_node.parent = self
        self.children.append(child_node)


class AudioConverter:
    def __init__(self, **kwargs):
        self.dbf_folder = kwargs.get("dbf_folder", "dbf/")
        self.audio_folder = kwargs.get("audio_folder", "audio/")
        self.output_folder = kwargs.get("output_folder", "output/")
        self.log_folder = kwargs.get("log_folder", "log/")
        self.artists_file = kwargs.get("artists_file", "artists.txt")
        self.artist_export_file = kwargs.get("artist_export_file", "artist_export.txt")
        self.tree_export_file = kwargs.get("tree_export_file", "tree_export.txt")
        self.tracks_export_file = kwargs.get("tracks_export_file", "tracks_export.txt")
        self.converted_files_folder = kwargs.get("converted_files_folder", "converted_files/")
        self.process_category = kwargs.get("process_category", "all")
        self.exclude_dbfs = kwargs.get("exclude_dbfs", "")
        self.sql_folder = kwargs.get("sql_folder", "sql/")
        self.include_folders = kwargs.get("include_folders", "")  # For mp3 folders
        self.chamgei_music_folder = kwargs.get("chamgei_music_folder", "")

        converted = kwargs.get("keep_converted", "False")
        if converted == "True":
            self.keep_converted = True
        else:
            self.keep_converted = False

        self.artists = self.fetch_data(self.artists_file)
        self.folders = {}

        self.mssql_con = self._make_mssql_connection()

        self.max_artist_id = self.get_max_artist_id()

        self.total_mts_files = 0
        self.total_converted_files = 0
        self.total_failed_conversions = 0
        self.total_failed_probes = 0
        self.total_missing_files = 0
        self.total_conversion_time = 0
        self.total_conversion_time_str = ""
        self.total_zero_bytes_files = 0


    def _make_mssql_connection(self):
        reg = read_registry()
        server = reg['server']
        database = reg['database']
        username = reg['username']  
        password = reg['password']
        return MSSQLData(server, database, username, password)

    def fetch_artists(self, file) ->dict[int, str]:
        # Check if the file exists
        if not os.path.exists(file):
            return {}

        # Read artists from a file with the following format : id, name
        with open(file, "r", encoding='utf-8') as f:
            data = f.read().split("\n")
            artists = {}
            for line in data:
                if line == "":
                    continue
                id, name = line.split(",")
                artists[name] = int(id)

        # Get max id for the artists
        max_id = max(artists.values())
        return artists

    def fetch_data(self, file:str) ->dict[int, str]:
        # Check if the file exists
        if not os.path.exists(file):
            return {}

        # Read artists from a file with the following format : id, name
        with open(file, "r", encoding='utf-8') as f:
            records = f.read().split("\n")
            data = {}
            for record in records:
                if record == "":
                    continue
                id, name = record.split("|")
                data[name] = int(id)
        return data

    def process_import_data(self):
        print("Processing import data")

        tree = self.fetch_data(self.tree_export_file)
        self.artists = self.fetch_data(self.artist_export_file)

        print(f"Processing data for {len(tree)} trees")
        print(f"Processing data for {len(self.artists)} artists")

        for name, id in tree.items():
            tracks = self.prepare_tracks_import_data(name, id)
            if len(tracks) > 0:
                stmts =self.make_sql_import_stmts(tracks)
                # Write data to a json file
                self.write_sql_stmts(stmts, name)

    def prepare_tracks_import_data(self, tree_name:str, tree_id:int) -> list:
        tracks = []

        converted_json_file = f"{self.log_folder}/{tree_name}_converted.json"
        if not os.path.exists(converted_json_file):
            return tracks

        # Load converted json file
        with open(converted_json_file, "r") as f:
            converted_data = json.load(f)

        for record in converted_data:
            track = {}
            track['tracktitle'] = record['title']
            track['artistsearch'] = record['artist']
            track['filepath'] = "//AUDIO-SERVER/"   # Find out the correct path
            track['class'] = 'SONG'
            track['duration'] = record['duration_ms']
            track['year'] = 2020
            track['fadein'] = 0 
            track['fadeout'] = 0
            track['fadedelay'] = 0
            track['intro'] = 0
            track['extro'] = 0
            track['folderid'] = tree_id
            track['onstartevent'] = -1
            track['onstopevent'] = -1
            track['disablenotify'] = 0
            track['physicalstorageused'] = record['converted_file_size_kb'] * 1024
            track['trackmediatype'] = 'AUDIO'

            if record['artist'] not in self.artists.keys():
                track['artistID_1'] = 0
            else:
                track['artistID_1'] = self.artists[record['artist']]

            track['old_filename'] = record['converted_filename']

            tracks.append(track)

        return tracks

    def make_sql_import_stmts(self, tracks:list)-> list:
        stmts = []
        for track in tracks:
            ins_stmt = (f'Insert into Tracks (tracktitle,artistsearch,filepath,class,duration,year,'
                        f'fadein,fadeout,fadedelay,intro,extro,folderid,onstartevent,onstopevent,'
                        f'disablenotify,physicalstorageused,trackmediatype,artistID_1, old_filename)'
                        f' VALUES ( '
                        f'"{track["tracktitle"]}","{track["artistsearch"]}","{track["filepath"]}", '
                        f'"{track["class"]}",{track["duration"]},{track["year"]},{track["fadein"]},{track["fadeout"]},'
                        f'{track["fadedelay"]},{track["intro"]},{track["extro"]},{track["folderid"]},'
                        f'{track["onstartevent"]},{track["onstopevent"]},{track["disablenotify"]},'
                        f'{track["physicalstorageused"]},"{track["trackmediatype"]}",{track["artistID_1"]},"{track["old_filename"]}" );')

            stmts.append(ins_stmt)

        return stmts

    def write_sql_stmts(self, stmts:list, tree_name:str) ->bool:
        filename = f"{self.sql_folder}/{tree_name}.sql"
        print(f"Writing data to: {filename}")

        try:
            with open(f"{filename}", "w", encoding='utf-8') as f:
                try:
                    for stmt in stmts:
                        f.write(stmt)
                        f.write("\n")
                except(IOError, OSError):
                    print(f"*ERROR* : Writing to file {filename}")
                    return False
        except (FileNotFoundError, PermissionError, OSError):
            print(f"*ERROR* : Opening file {filename}")
            return False

        return True

    def rename_converted_files(self):
        print("Renaming converted files")

        tracks = self.fetch_data(self.tracks_export_file)
        for old_name, id in tracks.items():
            new_name = f"{str(id).zfill(8)}.ogg"
            print(f"Old name: {old_name} =>  New name: {new_name}")
            try:
                os.rename(f"{self.output_folder}/{old_name}", f"{self.converted_files_folder}/{new_name}")
            except:
                print(f"Failed to rename {old_name} to {new_name}")
                continue


    def convert(self):
        # Loop through a folder and read all files with extension .dbf
        print(f"Reading data from......: {self.dbf_folder}")

        dbf_files = [f for f in os.listdir(self.dbf_folder+"/") if f.endswith('.DBF')]
        print(f"{len(dbf_files)} files found")

        if self.process_category != "all":
            proc_cats = self.process_category.split(",")
            pcats = [ cat.upper()+".DBF" for cat in proc_cats if cat.upper()+".DBF" in dbf_files ]

            if len(pcats) == 0:
                raise Exception(f"Invalid process category: {self.process_category}")

            dbf_files = pcats

        exclude_dbfs = []

        if self.exclude_dbfs != "":
            exclude_dbfs = self.exclude_dbfs.split(",")
            # Capitialize all elements in exclude_dbfs
            exclude_dbfs = [dbf.upper()+".DBF" for dbf in exclude_dbfs]

        for dbf in dbf_files:

            if dbf in exclude_dbfs:
                print(f"Excluding.......: {dbf}")
                continue

            print(f"Reading data from.......: {dbf}")
            data = get_data(self.dbf_folder,dbf, self.audio_folder)

            if len(data) == 0:
                continue

            print(f"{len(data)} records found")

            self.write_data(data, dbf)

            print(f"Converting audio files to ogg")
            self.convert_audio(data, dbf)

        self.print_summary()
    

    def convert_audio(self, data: list, dbf: str):
        # Convert audio files from MTS to ogg
        missing_files = []
        converted_files = []
        conversion_log = []
        failed_conversions = []
        failed_probes = []
        zero_bytes_files = []

        dbf = dbf[:-4]

        dbf_folder = f"{self.audio_folder}/{dbf}/"
        if not os.path.exists(dbf_folder):
            print(f"Missing audio folder: {dbf_folder}")
            return

        mts_files = [f for f in os.listdir(dbf_folder) if f.endswith('.MTS')]

        total_conversion_time = 0.0
        total_converted_files = 0

        for i, record in enumerate(data):
            # Get .MTS file count in the audio folder

            #input_file = f"{self.audio_folder}/{dbf}/{record['audio_file']}"
            input_file = record['audio_file']

            # Check if audio file exists
            if not os.path.exists(f"{input_file}"):
                missing_files.append(record)
                print(f"Missing audio file: {input_file}  ... skipping")
                continue

            file_in_bytes = 0
            try:
                # Get size in KB of input_file
                file_in_bytes = os.path.getsize(input_file)
            except OSError as e:
                print(f"Failed to get size of {input_file}: {e}")
                continue

            input_file_size_kb = file_in_bytes / 1024

            if input_file_size_kb == 0:
                zero_bytes_files.append(record)
                print(f"Zero bytes file: {input_file}  ... skipping")
                continue

            record["input_file_size_kb"] = input_file_size_kb

            # Check if output folder exists
            if not os.path.exists(self.output_folder):
                os.makedirs(self.output_folder)

            output_file = f"{record['category']}{record['code']}.ogg"
            output_filepath = f"{self.output_folder}//{output_file}"

            if self.keep_converted:
                if os.path.exists(output_filepath):
                    print(f"Output file already exists: {output_filepath}  ... skipping")
                    continue

            # Check if ffmpeg is installed
            if not os.path.exists("ffmpeg.exe"):
                raise Exception("ffmpeg is not installed")

            conversion_msg = f"{i+1}.Converting: {input_file} ({input_file_size_kb:.2f} KB) => {output_filepath}"
            print(conversion_msg, end="\r")
            
            audio_converted = False

            start_time = timer()

            try:
                os.system(f"ffmpeg -y -i {input_file} -nostats -loglevel 0 -c:a libvorbis -q:a 4 -vsync 2 {output_filepath}")
                audio_converted = True
            except:
                failed_conversions.append(record)
                continue

            end_time = timer()
            time_diff = timedelta(seconds=end_time - start_time)
            print(f"{conversion_msg}... Done. Time: {time_diff}")

            total_conversion_time += time_diff.total_seconds()
            total_converted_files += 1

            # Get size in KB of input_file
            try:
                output_file_size_kb = os.path.getsize(output_filepath) / 1024
            except OSError as e:
                print(f"Failed to get size of {output_filepath}: {e}")
                failed_conversions.append(record)
                continue

            record["conversion_time"] = time_diff.total_seconds()
            record["converted_filename"] = output_file
            record["converted_file_size_kb"] = output_file_size_kb

            duration = 0
            if audio_converted:
                try:
                    duration = self.probe_audio_duration(output_filepath)
                except:
                    failed_probes.append(record)

            record["duration_ms"] = duration * 1000 # milliseconds
            converted_files.append(record)

            # check if the record 'artist' is in the artists dictionary if not add it
            if record['artist'] not in self.artists:
                self.artists[record['artist']] = len(self.artists)

        print("")
        print(f"...................[ {dbf} ]........................")
        print(f"Category Files Converted.........: {total_converted_files}")

        print(f"Category Missing Files...........: {len(missing_files)}")

        print(f"Category Zero Byte Files.........: {len(zero_bytes_files)}")

        # Print total conversion time as "hh:mm:ss" format
        total_time = timedelta(seconds=total_conversion_time)
        print(f"Category conversion time.........: {total_time}")

        average_conversion_time = 0
        if total_converted_files > 0:
            # Get average conversion time in seconds
            average_conversion_time = total_conversion_time / total_converted_files

            # Print average conversion time as "hh:mm:ss" format
        average_conversion_time = timedelta(seconds=average_conversion_time)

        print(f"Average conversion time..........: {average_conversion_time}")

        print("................................................")
        print("")

        dt0 = datetime.datetime(1,1,1)

        log = {"category":dbf,
                "dbf_file":dbf+".DBF",
                "dbf_records":len(data),
                "audio_files":len(mts_files),
                "output_folder":self.output_folder,
                "converted_files_count":len(converted_files),
                "converted_files":converted_files,
                "missing_files_count":len(missing_files),
                "zero_bytes_files_count":len(zero_bytes_files),
                "zero_bytes_files":zero_bytes_files,
                "total_conversion_time":(dt0+total_time).strftime('%H:%M:%S'), 
                "average_conversion_time":(dt0+average_conversion_time).strftime('%H:%M:%S'),
                "failed_conversions":failed_conversions,
                "failed_probes":failed_probes,
                "missing_files":missing_files
                }

        self.total_mts_files += len(mts_files)
        self.total_converted_files += len(converted_files)
        self.total_failed_conversions += len(failed_conversions)
        self.total_failed_probes += len(failed_probes)
        self.total_missing_files += len(missing_files)
        self.total_zero_bytes_files += len(zero_bytes_files)
        self.total_conversion_time += total_time.total_seconds()
        self.total_conversion_time_str = (dt0+timedelta(seconds=self.total_conversion_time)).strftime('%H:%M:%S')

        conversion_log.append(log)
        
        # Write converted files to a json file
        # Remove extension .DBF
        with open(f"{self.log_folder}/{dbf}_converted.json", "w") as f:
            json.dump(converted_files, f, indent=4) 
        
        # Write missing files to a json file
        # Remove extension .DBF
        with open(f"{self.log_folder}/{dbf}_missing.json", "w") as f:
            json.dump(missing_files, f, indent=4)

        # Write zero byte files to a json file
        # Remove extension .DBF
        with open(f"{self.log_folder}/{dbf}_zero_bytes.json", "w") as f:
            json.dump(zero_bytes_files, f, indent=4)

        # Write conversion log to a json file
        with open(f"{self.log_folder}/{dbf}_conversion_log.json", "w") as f:
            json.dump(conversion_log, f, indent=4)

        # Write artists to the artists text file, if the artist file does not exists, create it
        with open(f"{self.log_folder}/artists.txt", "w") as f:
            for artist, id in self.artists.items():
                f.write(f"{id}|{artist}\n")

    def probe_audio_duration(self, audio_file: str)-> float:
        # Get audio duration in seconds
        result = run(["ffprobe", "-v", "quiet", "-show_entries", "format=duration", "-of", "default=noprint_wrappers=1:nokey=1", audio_file], stdout=PIPE, stderr=PIPE)
        return float(result.stdout.decode("utf-8"))

    def print_summary(self):
        print(f"Total MTS files...............: {self.total_mts_files}")
        print(f"Total converted files.........: {self.total_converted_files}")
        print(f"Total failed conversions......: {self.total_failed_conversions}")
        print(f"Total failed probes...........: {self.total_failed_probes}")
        print(f"Total missing files...........: {self.total_missing_files}")
        print(f"Total zero bytes files........: {self.total_zero_bytes_files}")
        print(f"Total conversion time.........: {self.total_conversion_time_str}")

    def write_data(self, data: list, dbf: str):
        # Write data to a json file, remove extension .DBF
        dbf = dbf[:-4]
        print(f"Writing data to: {self.dbf_folder}/{dbf}.json")
        with open(f"{self.dbf_folder}/{dbf}.json", "w") as f:
            json.dump(data, f, indent=4)


    def list_audio_files(self):
        # Read data from the audio database
        print("Listing audio files from database")

        if not self.mssql_con.connect():
            print("Failed to connect to database")
            return

        sql = (f'select Tracks.filepath, Tracks.TrackReference, Tracks.TrackTitle, '
              f' Tracks.ArtistSearch, Tree.NodeName, Tracks.Duration '
              f' from Tracks, Tree '
              f'Where tracks.FolderID = Tree.NodeID '
              f'order by TrackReference ')

        cursor = self.mssql_con.conn.cursor()
        cursor.execute(sql)
        rows = cursor.fetchall()

        trcks = []

        for index, row in enumerate(rows):
            track =  {}

            print(f"Processing record {index+1} / {len(rows)}", end="\r")

            filepath = row[0]
            track_reference = row[1]
            track_title = row[2]
            artist_search = row[3]
            node_name = row[4]
            duration = row[5]
            ogg_filename = f"{str(track_reference).zfill(8)}.ogg"
            ogg_filepath = f"{filepath}{ogg_filename}"

            # Generate hash of track_reference + track_title + artist_search
            if track_title is None:
                continue
            if artist_search is None:
                continue
            unique_id = hashlib.sha256(str(track_reference).encode() + track_title.encode()).hexdigest()[0:24]

            # Create hash of ogg_filepath
            song_id = hashlib.sha256(ogg_filename.encode()).hexdigest()[0:32]

            track["unique_id"] = unique_id
            track["song_id"] = song_id
            track["path"] = ogg_filepath
            track["title"] = track_title
            track["artist"] = artist_search
            track["album"] = ""
            track["genre"] = node_name
            track["lyrics"] = ""
            track["isrc"] = ""
            track["playlist"] = "default"
            track["length"] = duration
            track["amplify"] = ""
            track["fade_in"] = ""
            track["fade_out"] = ""
            track["cue_in"] = ""
            track["cue_out"] = ""
            track["cross_start_next"] = ""

            trcks.append(track)

        self.mssql_con.disconnect()

        # Write data to a csv file
        filename = f"{self.log_folder}/audio_files_list.csv"
        print(f"Writing audio files list to: {filename}")
        with open(filename, "w", encoding='utf-8') as f:
            # Write header
            f.write("unique_id,song_id,path,title,artist,album,genre,lyrics,isrc,playlist,length,amplify,fade_in,fade_out,cue_in,cue_out,cross_start_next\n")
            for track in trcks:
                f.write(f"{track['unique_id']},{track['song_id']},"
                f"{track['path']},{track['title']},{track['artist']},"
                f"{track['album']},{track['genre']},{track['lyrics']},"
                f"{track['isrc']},{track['playlist']},{track['length']},"
                f"{track['amplify']},{track['fade_in']},{track['fade_out']},"
                f"{track['cue_in']},{track['cue_out']},{track['cross_start_next']}\n")
            
        print("Audio files listing done.")

    def convert_mp3_to_ogg(self):
        if not os.path.exists("ffmpeg.exe"):
            raise Exception("ffmpeg is not installed")

        self.artists = self.read_artists_from_db()
        self.folders = self.read_track_folders_from_db()

        mp3_folders = [dir for dir in os.listdir(f"{self.audio_folder}")
                        if os.path.isdir(os.path.join(self.audio_folder, dir))]

        audio_folders = {}

        include_folders = self.include_folders.split(",")

        for mp3_folder in mp3_folders:

            short_folder_name =  mp3_folder[:mp3_folder.index("-")].strip()
            if len(include_folders) > 0:
                if short_folder_name not in include_folders:
                    continue

            folder_id = self.folders[short_folder_name]
            filepath = f"{self.audio_folder}/{mp3_folder}"

            folder = {'folder_id': folder_id,
                    'folder_short_name': short_folder_name,
                    'filepath': filepath}

            mp3_files = self.read_mp3_audio_files(folder)

            audio_folders[short_folder_name] = mp3_files

        max_track_id = self.get_max_track_id()
        print(f"Current Max Track ID....: {max_track_id}")

        counter = 0

        for folder, files in audio_folders.items():

            counter += 1

            print(f"Processing folder {folder}...")

            converted_files = []

            for file in files:

                output_filepath =  self.make_output_filename(file)
                file['output_filepath'] = output_filepath

                # Check file size
                mp3_file = file['full_filepath']
                if self.get_file_size(mp3_file) == 0:
                    print(f"Zero bytes file: {mp3_file} ...Skipping.")
                    continue

                if self.keep_converted:
                    if os.path.exists(output_filepath):
                        print(f"Output file already exists: {output_filepath}  ... skipping")
                        continue

                if self.mp3_to_ogg(file):
                    converted_files.append(file)
                else:
                    self.failed_conversions.append(file)
        
            for converted_file in converted_files:
                max_track_id += 1
                filepath = self.output_folder  

                output_filepath = converted_file['output_filepath']
                ogg_filepath = self.make_ogg_filepath(filepath, max_track_id)

                if not self.rename_converted_file_to_ogg(output_filepath, ogg_filepath):
                    print(f"Failed to rename {output_filepath} to {ogg_filepath}")
                    converted_file['ogg_filepath'] =""
                    max_track_id -= 1
                    converted_file['track_id'] = -1
                else:
                    converted_file['ogg_filepath'] = ogg_filepath
                    converted_file['track_id'] = max_track_id

            if not self.write_artists_insert_stmts_to_file(folder, counter):
                print(f"Failed to write Artists insert statements for folder: {folder} ")
                print(f"Process terminated.")
                return

            if not self.write_tracks_insert_stmts_to_file(folder, converted_files):
                print(f"Failed to write Tracks insert statments for folder: {folder}")
                print(f"Process terminated.")
                return

        print(f"File conversion done.")
        print(f"Last folder: {folder}")

    def write_tracks_insert_stmts_to_file(self, folder: str, converted_files:dict) ->bool:
        # Generated SQL insert statements
        conv_files = [cf for cf in converted_files if cf['ogg_filepath'] != ""]

        print(f"Writing DB statements for `{folder}` ...")

        sql_stmts = self.generate_insert_statements(conv_files)

        return self.write_sql_stmts(sql_stmts, folder)


    def write_artists_insert_stmts_to_file(self, folder:str, counter: int):
        new_artists = []

        for artist_name, data in self.artists.items():
            if data['in_db']:
                continue
            new_artists.append({'id': data['id'], 'name':artist_name})

        print(f"Generating DB statements for artists ...")
        sql_stmts = self.generate_artists_insert_stmts(new_artists)

        filename = f"artists_{folder}_{counter}"
        print(f"Writting `{filename}` file ...")
        return self.write_sql_stmts(sql_stmts, filename)


    def generate_insert_statements(self, conv_files: list):
        audio_folder = fr"{self.get_audio_folder()}"

        audio_folder = audio_folder.replace("\\\\", "\\")

        stmts = []
        for cf in conv_files:

            if cf['track_id'] == -1:
                continue

            print(cf)

            file_size = self.get_file_size(cf['ogg_filepath'])

            ins_stmt = (f'Insert into Tracks (trackreference, tracktitle,artistsearch,filepath,class,duration,year,'
                        f'fadein,fadeout,fadedelay,intro,extro,folderid,onstartevent,onstopevent,'
                        f'disablenotify,physicalstorageused,trackmediatype,artistID_1)'
                        f' VALUES ( '
                        f'{cf["track_id"]},"{cf["title"]}","{cf["artist"]}","{audio_folder}", '
                        f'"SONG",{cf["duration"]},2025,0,0,0,0,0,{cf["folder_id"]},-1,-1,0,'
                        f'{file_size},"AUDIO",{cf["artist_id"]});')

            stmts.append(ins_stmt)
        return stmts


    def generate_artists_insert_stmts(self, artists: list) -> list:
        stmts = []
        for artist in artists:
            stmt = (f'Insert into Artists (ArtistID, ArtistSurname, ArtistType) '
                         f'VALUES ({artist["id"]}, "{artist["name"]}", "GROUP" );')

            stmts.append(stmt)

        return stmts

    def mp3_to_ogg(self, file) ->bool:
        input_filepath = file['full_filepath']
        output_filepath = file['output_filepath']

        print(f"Converting file...: {input_filepath} => {output_filepath}")
        try:
            os.system(f'ffmpeg -y -i "{input_filepath}" -nostats -loglevel 0 -c:a libvorbis -q:a 4 -vsync 2 "{output_filepath}"')
            return True
        except:
            return False

    def make_output_filename(self, file: dict) ->str:
        mp3_filename = file['mp3_filename']
        filepath = self.output_folder   
        return f"{filepath}/{mp3_filename[:-4]}.OGG"

    def make_ogg_filepath(self, filepath:str, track_id:int) ->str:
        ogg_filename = f"{str(track_id).zfill(8)}.ogg"
        ogg_filepath = f"{filepath}/{ogg_filename}"
        return ogg_filepath

    def rename_converted_file_to_ogg(self, old_file: str, new_file:str) ->bool:
        try:
            os.rename(old_file, new_file)
            return True
        except:
            print(f"Failed to rename {old_file} to {new_file}")
            return False

    def get_max_track_id(self):
        if not self.mssql_con.connect():
            print("Failed to connect to database")
            return
        cursor = self.mssql_con.conn.cursor()
        cursor.execute("SELECT max(TrackReference) max_id FROM Tracks")
        rows = cursor.fetchall()

        for row in rows:
            max_id = row[0]

        self.mssql_con.disconnect()

        return max_id

    def get_max_artist_id(self):
        if not self.mssql_con.connect():
            print("Failed to connect to database")
            return

        cursor = self.mssql_con.conn.cursor()
        cursor.execute("SELECT max(ArtistID) max_id FROM Artists")
        rows = cursor.fetchall()

        for row in rows:
            max_id = row[0]

        self.mssql_con.disconnect()

        return max_id


    def get_file_size(self, file:str) ->float:
        file_in_bytes = 0
        try:
            # Get size in KB of input_file
            file_in_bytes = os.path.getsize(file)
        except OSError as e:
            print(f"Failed to get size of {file}: {e}")
            return 0

        input_file_size_kb = file_in_bytes / 1024
        return input_file_size_kb


    def read_artists_from_db(self) ->dict:
        # Read artists from the database
        if not self.mssql_con.connect():
            print("Failed to connect to database")
            return

        cursor = self.mssql_con.conn.cursor()
        cursor.execute("SELECT ArtistID, ArtistSurname FROM Artists")
        rows = cursor.fetchall()

        ARTIST_ID = 0
        ARTIST_NAME = 1

        artists = {}
        for row in rows:
            artists[row[ARTIST_NAME]] = {'id':row[ARTIST_ID], 'in_db':True}

        self.mssql_con.disconnect()

        return artists


    def read_track_folders_from_db(self) ->dict:
        # Read tree from the database
        if not self.mssql_con.connect():
            print("Failed to connect to database")
            return

        cursor = self.mssql_con.conn.cursor()
        cursor.execute("SELECT NodeID, NodeName FROM Tree")
        rows = cursor.fetchall()

        NODE_ID = 0
        NODE_NAME = 1

        folders = {}
        for row in rows:
            folders[row[NODE_NAME]] = row[NODE_ID]  

        self.mssql_con.disconnect()

        return folders
    
    def get_audio_folder(self):
        if not self.mssql_con.connect():
            print("Failed to connect to database")
            return

        cursor = self.mssql_con.conn.cursor()
        cursor.execute("SELECT DefRecordLocation FROM System")
        rows = cursor.fetchall()

        audio_location = ""

        for row in rows:
            audio_location = row[0]

        self.mssql_con.disconnect()

        return audio_location


    def read_mp3_audio_files(self, mp3_folder:str) ->list:
        folder_id = mp3_folder['folder_id']
        filepath = mp3_folder['filepath']
        folder_short_name = mp3_folder['folder_short_name']

        mp3_raw_files = [f for f in os.listdir(filepath) if f.endswith('.mp3')]

        data_files = []

        for mp3_file in mp3_raw_files:
            full_filepath = f"{filepath}/{mp3_file}"

            print(f"Probing file: {full_filepath}")

            data = self.probe_mp3_file(full_filepath)

            if len(data) == 0:
                continue

            if not "title" in data.keys():
                # Get title from the filename
                data['title'] = mp3_file[:-4]
                #continue

            if not "artist" in data.keys():
                continue

            data['folder_id'] = folder_id
            data['folder_short_name'] = folder_short_name
            data['mp3_filename'] = mp3_file
            data['filepath'] = filepath
            data['full_filepath'] = full_filepath

            data_files.append(data)
        
        return data_files


    def probe_mp3_file(self, filepath) -> dict:

        if not os.path.exists(filepath):
            return

        try:
            cmd = f'ffprobe.exe -i "{filepath}" -show_format -v quiet | grep -E "title|artist|duration"'
            result = run(cmd, capture_output=True, shell=True, text=True, encoding='utf-8')
            data_str = result.stdout
            data_str = data_str.replace("TAG:", "")
            data_str = data_str.replace(":", " ")
            data_str = data_str.replace("=", ":")
        except:
            print(f"Error probing file: {filepath} ")
            return {}

        # loop through the string and split it into key value pairs
        # split the string by ":" and loop through the string
        # and split it into key value pairs

        data = {}
        key = ""

        data_values = data_str.split("\n")

        for data_value in data_values:
            if data_value == "":
                continue

            key, value = data_value.split(":")

            # Remove all return characters and new lines
            value = value.replace("\n", "").replace("\r", "").strip()
            key = key.strip()

            if (key == "duration"):
                # Convert duration to milliseconds
                value = float(value)
                value = int(value * 1000)

            if (key == 'artist'):
                # Check if the artist is in the artists dictionary
                if value not in self.artists.keys():
                    print(f"Missing artists: {value}")
                    # Add the artist to the artists dictionary
                    self.max_artist_id = self.max_artist_id + 1
                    self.artists[value] = {'id':self.max_artist_id, 'in_db':False}
                    data["artist_id"] = self.max_artist_id
                else:
                    artist_id = self.artists[value]['id']
                    data["artist_id"] = artist_id

            data[key] = value

        # If we dont have an artist, we create a default one called "Unknown Artist"
        if "artist_id" not in data.keys():
            if not "Unknown Artist" in self.artists.keys():
                self.max_artist_id += 1
                data["artist_id"] = self.max_artist_id
                self.artists["Unknown Artist"] = {'id': self.max_artist_id, 'in_db': False}
            else:
                artist_id = self.artists["Unknown Artist"]['id']
                data["artist_id"] = artist_id

            data["artist"] = "Unknown Artist"

        data["filepath"] = filepath

        return data


    def walk_mp3_folders(self, root_folder:str) -> list:
        all_data = []
        folder_struct = {}
        folder_struct[root_folder] = {}
        for dirpath, dirnames, filenames in os.walk(root_folder):
            print(f"Processing folder: {dirnames}")
            for filename in filenames:
                if filename.endswith('.mp3'):
                    full_path = os.path.join(dirpath, filename)
                    print(f"Probing file: {full_path}")
                    # data = self.probe_mp3_file(full_path)
                    # if data:
                    #     all_data.append(data)
        return all_data

    def build_tree(self, path: Path, parent=None) -> TreeNode:
        # Only include folders and .mp3 files
        if path.is_file():
            if path.suffix.lower() != '.mp3':
                return None
            is_file = True
        else:
            is_file = False

        node = TreeNode(path.name, is_file=is_file, filepath=path, parent=parent)
        if path.is_dir():
            print(f"Reading folder: {path}")
            for child_path in sorted(path.iterdir(), key=lambda p: (p.is_file(), p.name)):
                child_node = self.build_tree(child_path, parent=node)
                if child_node is not None:
                    node.add_child(child_node)
        return node

    def extract_tree(self, node: TreeNode) -> dict:
        tree_list = {}
        tree_list[node.node_id] = {
            'name': node.name,
            'is_file': node.is_file,
            'parent_id': node.parent.node_id if node.parent else None,
            'children_ids': [child.node_id for child in node.children],
            'filepath': str(WindowsPath(node.filepath)),
            'outputfilepath':f"{self.output_folder}/{node.node_id:08d}.ogg" if node.is_file else None
        }
        for child in node.children:
            tree_list.update(self.extract_tree(child))
        return tree_list

    def print_tree(self, node: TreeNode, indent: str = ""):
        print(indent + ("📄 " if node.is_file else "📁 ") + node.name + "("+ (str(node.node_id) + ":" + str(node.parent.node_id) if node.parent else "root") +")")
        for child in node.children:
            self.print_tree(child, indent + "    ")

    def print_tree_with_counts(self, node: TreeNode, indent: str = ""):
        # Print current node with file/folder icon
        print(indent + ("📄 " if node.is_file else "📁 ") + node.name)

        # Initialize counts for this subtree
        folder_count = 0
        file_count = 0

        # Current node counts as folder if not a file
        if node.is_file:
            file_count += 1
        else:
            folder_count += 1

        # Recurse into children and aggregate counts
        for child in node.children:
            child_folders, child_files = self.print_tree_with_counts(child, indent + "    ")
            folder_count += child_folders
            file_count += child_files

        return folder_count, file_count

    def prepare_files_for_conversion(self, root_folder: str):
        if root_folder == "":
            print("Please provide a valid root folder.")
            return

        path = Path(root_folder)
        root_node = self.build_tree(path)
        tree_list = {}

        print("Extracting tree structure to CSV format...")

        tree_list = self.extract_tree(root_node)
        files = []
        folders = []    

        for node_id, item in tree_list.items():
            if node_id == 1:
                print(f"Root folder: {item['name']} (ID: {node_id}) {item['is_file']}")
            if item['is_file']:
                row = self.make_row_dict(node_id, item)
                files.append(row)
            else:
                row = f"{node_id}|{item['name']}| {item['parent_id']}|0|0|0|0|1|null"
                folders.append(row)

        self.write_files("folders.csv", folders)

        print(f"Length of artist data: {len(self.artists)}")
        self.write_artists_to_file()

        # save files as a JSON file
        with open("files.json", "w", encoding="utf-8") as f:
            json.dump(files, f, ensure_ascii=False, indent=4)

        print("Finished extracting tree structure to JSON format.")
        print(f"Total Folders: {len(folders)}")
        print(f"Total Files: {len(files)}")

        #self.print_tree(root_node)
        # folders, files = self.print_tree_with_counts(root_node)
        # print(f"Folders: {folders}")
        # print(f"Files: {files}")

    def convert_prepared_files(self):
        print("Converting prepared files...")

        json_file = "files.json"
        if not os.path.exists(json_file):
            print(f"File not found: {json_file}")
            return

        with open(json_file, "r", encoding="utf-8") as f:
            files = json.load(f)

        print(f"Converting {len(files)} files...")
        start_time = datetime.datetime.now()
        print(f"Start Time: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")

        insert_statements = []
        failed_conversions = []
        for index, file in enumerate(files):
            input_file = file['filepath']
            node_id = file['node_id']
            output_file = f"{self.output_folder}/{str(node_id).zfill(8)}.ogg"

            if not os.path.exists(input_file):
                print(f"Input file not found: {input_file} ... skipping")
                continue

            if self.keep_converted:
                if os.path.exists(output_file):
                    print(f"Output file already exists: {output_file}  ... skipping")
                    continue

            print(f"{index}. Converting: {input_file} => {output_file}")

            try:
                os.system(f"ffmpeg -y -i \"{input_file}\" -nostats -loglevel 0 -c:a libvorbis -q:a 4 -vsync 2 \"{output_file}\"")
                print(f"Converted: {input_file} => {output_file}")
                duration =  self.probe_audio_duration(output_file)
                file['duration'] = int(duration * 1000)  # in milliseconds
                file['physicalstorageused'] = self.get_file_size(output_file)
                track_insert_stmt = self.make_insert_statement(file)
                insert_statements.append(track_insert_stmt)
            except:
                print(f"Failed to convert: {input_file} => {output_file}")
                failed_conversions.append(file)
                continue

        print(f"Writing insert statements to file...{len(insert_statements)}")
        self.write_stmts(insert_statements)

        # Write failed conversions to a json file
        print(f"Writing failed conversions to file...{len(failed_conversions)}")
        if len(failed_conversions) > 0:
            with open(f"{self.log_folder}/failed_conversions.json", "w", encoding="utf-8") as f:
                json.dump(failed_conversions, f, ensure_ascii=False, indent=4)

        print("File conversion done.")
        end_time = datetime.datetime.now()
        print(f"End Time: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        time_diff = end_time - start_time
        print(f"Total Conversion Time: {time_diff}")


    def write_stmts(self, stmts: list):
        filename = f"{self.log_folder}/tracks_insert_statements.sql"
        try:
            with open(filename, "w", encoding="utf-8") as f:
                for stmt in stmts:
                    try:
                        f.write(stmt + "\n")
                    except:
                        print(f"*ERROR* : Writing to file {filename}")
                        return False
            return True
        except:
            print(f"*ERROR* : Creating file {filename}")
            return False    


    def make_insert_statement(self, file: dict) -> str:
        ins_stmt = (f'Insert into Tracks (trackreference, tracktitle,artistsearch,filepath,class,duration,year,'
                    f'fadein,fadeout,fadedelay,intro,extro,folderid,onstartevent,onstopevent,'
                    f'disablenotify,physicalstorageused,trackmediatype,artistID_1, old_filename)'
                    f' VALUES ( '
                    f'{file["node_id"]},"{file["title"]}","{file["artist"]}","//AUDIO-SERVER", '
                    f'"{file["class"]}",{file["duration"]},{file["year"]},{file["fadein"]},{file["fadeout"]},'
                    f'{file["fadedelay"]},{file["intro"]},{file["extro"]},{file["folderid"]},'
                    f'{file["onstartevent"]},{file["onstopevent"]},{file["disablenotify"]},'
                    f'{file["physicalstorageused"]},"{file["trackmediatype"]}",{file["artistID_1"]}, {file["old_filename"]} );')
        return ins_stmt

    def make_row_dict(self, node_id: int, item: dict) -> dict:
        row = {}
        row['node_id'] = node_id
        row['name'] = item['name']

        # JACKSON  LELEI  CHEBWARENG  - TUGUKAB CHUMBEK.mp3
        # Split the name into artist and title based on the "-" character. If there are multiple
        # "-" characters, the last one separates the artist from the title.

        if "-" in item['name']:
            # Split by "-" and use the last part as title, the rest as artist
            parts = item['name'].rsplit("-", 1)
            if len(parts) == 2:
                row['artist'] = parts[0].strip()
                row['title'] = parts[1].strip().replace(".mp3", "")
            else:
                row['artist'] = parts[0].strip()
                row['title'] = parts[0].strip().replace(".mp3", "")
        else:
            row['artist'] = "UNKNOWN"
            row['title'] = item['name'].strip().replace(".mp3", "")

        row['parent_id'] = item['parent_id']
        row['filepath'] = item.get('filepath','')
        row['outputfilepath'] = item.get('outputfilepath','')
        row['class'] = "SONG"
        row['year'] = 2025
        row['fadein'] = 0
        row['fadeout'] = 0
        row['fadedelay'] = 0
        row['intro'] = 0
        row['extro'] = 0
        row['folderid'] = item['parent_id']
        row['onstartevent'] = -1
        row['onstopevent'] = -1
        row['disablenotify'] = 0
        row['physicalstorageused'] = 0
        row['trackmediatype'] = "AUDIO"
        row['old_filename'] = node_id
        artist = row['artist']
        if artist not in self.artists.keys():
            artist_id = len(self.artists) + 1
            self.artists[artist] = artist_id
            row['artist_id'] = artist_id
        else:
            row['artist_id'] = self.artists[artist]

        row['artistID_1'] = row['artist_id']
        return row

        # Step 1: Walk through the directory and build the tree structure
    def write_files(self, filename: str, lines: list):
        with open(filename, "w", encoding="utf-8") as f:
            for line in lines:
                f.write(line + "\n")

    def write_artists_to_file(self):
        print(f"Writing artists data...{len(self.artists)}")
        with open(f"{self.log_folder}/artists.txt", "w", encoding="utf-8") as f:
            for artist, id in self.artists.items():
                try:
                    f.write(f"{id}|{artist}\n")
                except:
                    print(f"*ERROR* : Writing artist: {artist} to file")
                    continue