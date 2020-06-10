import os
import os.path
import glob

restaurants = ["MenuPages", "NYMag", "NYTimes", "ActiveDiner", "NewYork", "DiningGuide", "OpenTable", "TimeOut", "FoodBuzz", "SavoryCities", "VillageVoice", "TasteSpace"]

def groupAndSepare(dir, out_dir):
    out_files = set()
    for file in os.listdir(dir):
        if file.endswith(".txt"):
            for rest in restaurants:
                restFile = out_dir + rest + ".txt"
                if restFile not in out_files:
                    out_files.add(restFile)
                with open(dir+file, 'r', encoding='windows-1252') as f:
                    targets = [line for line in f if rest in line]
                    with open(restFile, 'a', encoding='utf-8') as f_to_w:
                        f_to_w.writelines(targets)
    return out_files

def removeFullLineDuplicates(files):
     for file in files:
        if file.endswith(".txt"):
            lines_seen = set() # holds lines already seen
            striped_lines_seen = set() # holds striped lines already seen
            path = os.path.dirname(file)
            removed = 0
            for each_line in open(file, "r"):
                if each_line.strip() not in striped_lines_seen: # check if line is not duplicate
                    lines_seen.add(each_line)
                    striped_lines_seen.add(each_line.strip())
                else:
                    removed += 1
            print(f'{os.path.basename(file)}: {removed} lines removed!')
            with open(path + "/Output_file.txt", "w") as output_file:
                for each_line in sorted(lines_seen):
                    output_file.write(each_line)
            os.remove(file)
            os.rename(path + "/Output_file.txt", file)

def main():
    in_dir = "./data/restaurants/original/"
    out_dir = "./data/restaurants/grouped_by_restaurant/"

    # Cleanup out_dir
    files = glob.glob(out_dir+'*')
    for f in files:
        os.remove(f)

    rest_files = groupAndSepare(in_dir, out_dir)
    removeFullLineDuplicates(rest_files)

if __name__ == "__main__":
	main()
