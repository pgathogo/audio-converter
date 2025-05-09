import os
import json
from timeit import default_timer as timer
import datetime
from datetime import timedelta

from dbf_reader import get_data

from subprocess import PIPE, run

from mssql_data import MSSQLData, read_registry

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

        converted = kwargs.get("keep_converted", "False")
        if converted == "True":
            self.keep_converted = True
        else:
            self.keep_converted = False

        self.artists = self.fetch_data(self.artists_file)
        self.folders = {}

        self.mssql_con = self._make_mssql_connection()

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
        with open(file, "r") as f:
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
        with open(file, "r") as f:
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
            track['filepath'] = "//AUDIO-SERVER/AUDIO/"   # Find out the correct path
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

    def write_sql_stmts(self, stmts:list, tree_name:str):
        filename = f"{self.sql_folder}/{tree_name}.sql"
        print(f"Writing data to: {filename}")
        with open(f"{filename}", "w") as f:
            for stmt in stmts:
                f.write(stmt)
                f.write("\n")

    def rename_converted_files(self):
        print("Renaming converted files")

        tracks = self.fetch_data(self.tracks_export_file)
        for old_name, id in tracks.items():
            new_name = f"{str(id).zfill(8)}.ogg"
            print(f"Old name: {old_name}  New name: {new_name}")
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
            data = get_data(self.dbf_folder+"/"+dbf)

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

            input_file = f"{self.audio_folder}/{dbf}/{record['audio_file']}"

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

            output_file = f"{dbf}{record['code']}.ogg"
            output_filepath = f"{self.output_folder}/{output_file}"

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
        # Write data to a json file, remoce extension .DBF
        dbf = dbf[:-4]
        print(f"Writing data to: {self.dbf_folder}/{dbf}.json")
        with open(f"{self.dbf_folder}/{dbf}.json", "w") as f:
            json.dump(data, f, indent=4)

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

        converted_files = []

        for folder, files in audio_folders.items():
            for file in files:

                output_filepath =  self.make_output_filename(file)
                file['output_filepath'] = output_filepath

                # Check file size
                mp3_file = file['full_filepath']
                if self.file_size(mp3_file) == 0:
                    print(f"Zero bytes file: {mp3_file} ...Skipping.")
                    continue

                if self.keep_converted:
                    if os.path.exists(output_filepath):
                        print(f"Output file already exists: {output_filepath}  ... skipping")
                        continue

                # BEGIN-TESTING
                #converted_files.append(file)
                #continue
                # END-TESTING

                if self.mp3_to_ogg(file):
                    converted_files.append(file)
                else:
                    self.failed_conversions.append(file)

        max_track_id = self.get_max_track_id()
        print(f"Current Max Track ID....: {max_track_id}")
        
        for converted_file in converted_files:
            max_track_id += 1
            filepath = self.output_folder  #converted_file['filepath']

            output_filepath = converted_file['output_filepath']
            ogg_filepath = self.make_ogg_filepath(filepath, max_track_id)

            #TEST::BEGIN REMOVE
            #converted_file['ogg_filepath'] = ogg_filepath
            #converted_file['track_id'] = max_track_id
            #continue
            # TEST::END

            if not self.rename_converted_file_to_ogg(output_filepath, ogg_filepath):
                print(f"Failed to rename {output_filepath} to {ogg_filepath}")
                converted_file['ogg_filepath'] =""
                max_track_id -= 1
                converted_file['track_id'] = -1
            else:
                converted_file['ogg_filepath'] = ogg_filepath
                converted_file['track_id'] = max_track_id

        # Generated SQL insert statements
        conv_files = [cf for cf in converted_files if cf['ogg_filepath'] != ""]

        for folder in include_folders:
            print(f"Writing DB statements for `{folder}` ...")
            folder_files = [cf for cf in conv_files if cf['folder_short_name'] == folder]

            sql_stmts = self.generate_insert_statements(folder_files)
            self.write_sql_stmts(sql_stmts, folder)
        
        # Write insert statements for new artist
        new_artists = []

        for artist_name, data in self.artists.items():
            if data['in_db']:
                continue
            new_artists.append({'id': data['id'], 'name':artist_name})

        print(f"Generating DB statements for artists ...")
        sql_stmts = self.generate_artists_insert_stmts(new_artists)

        print(f"Writting `ARTISTS.sql' file ...")
        self.write_sql_stmts(sql_stmts, "ARTISTS")

        print(f"Processing done.")


    def generate_insert_statements(self, conv_files: list):
        audio_folder = fr"{self.get_audio_folder()}"

        audio_folder = audio_folder.replace("\\\\", "\\")

        stmts = []
        for cf in conv_files:

            if cf['track_id'] == -1:
                continue

            file_size = self.file_size(cf['ogg_filepath'])

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
                         f'VALUES ("{artist["id"]}", "{artist["name"]}", "GROUP" );')

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

    def make_output_filename(self, file) ->str:
        mp3_filename = file['mp3_filename']
        filepath = self.output_folder   #file['filepath']
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

    def file_size(self, file) ->float:
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

            data = self.probe_mp3_file(full_filepath)

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

        cmd = f'ffprobe.exe -i "{filepath}" -show_format -v quiet | grep -E "title|artist|duration"'
        result = run(cmd, capture_output=True, shell=True, text=True)
        data_str = result.stdout
        data_str = data_str.replace("TAG:", "")
        data_str = data_str.replace("=", ":")

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
                    artist_id = len(self.artists) + 1
                    self.artists[value] = {'id':artist_id, 'in_db':False}
                    data["artist_id"] = artist_id
                else:
                    artist_id = self.artists[value]['id']
                    data["artist_id"] = artist_id

            data[key] = value

        data["filepath"] = filepath

        return data
    
  

