#!/usr/bin/env python3
from partisan.irods import (
    Collection,
    DataObject,
    AVU
)
import logging

technologies = ["illumina", "pacbio", "ont", "sequenom", "fluidigm"]


def rm_or_keep(landf, existing, out):
    logging.info(f"exists at {existing}")
    if existing.checksum() == landf.checksum():
        logging.info(f"has correct checksum: {landf.checksum()} == {existing.checksum()}")
        if AVU("md5", existing.checksum()) in existing.metadata():
            logging.info(f"has correct checksum metadata")
            out.write(f"irm {landf.path}/{landf.name} # md5 ok, md5 meta ok, exists as {existing.path}/{existing.name}\n")
        else:
            logging.warning(f"does not have correct checksum metadata")
            out.write(f"# md5 ok, md5 meta NOT OK, exists as {existing.path}/{existing.name}\n")
    else:
        logging.warning(f"has incorrect checksum: {landf.checksum()} != {existing.checksum()}")
        out.write(f"# md5 NOT OK, exists as {existing.path}/{existing.name}\n")


def non_illumina(technology, obj):
    path = obj.path
    name = obj.name
    real_path = f"/seq/{technology}{str(path).split(technology)[1]}"
    match = DataObject(f"{real_path}/{name}")
    with open("resolve_orphaned_files.sh", "a") as out:
        if match.exists():
            rm_or_keep(obj, match, out)
        else:
            logging.info("does not exist")
            if Collection(real_path).exists():
                logging.info(f"moving to {real_path}/{name}")
                out.write(f"imv {path}/{name} {real_path}/{name} # {name} not present, runfolder at {real_path}\n")
                return True
            else:
                logging.info(f"no {technology} runfolder for this run")
                return False


def main():
    logging.basicConfig(filename='orphan_files.log', encoding='utf-8', level=logging.INFO)
    lost_and_found = Collection("/seq/lostandfound")

    for obj in lost_and_found.contents(recurse=True):
        if type(obj) == DataObject:
            path = obj.path
            name = obj.name
            technology = None
            for t in technologies:
                if t in str(path).lower():
                    technology = t
            if technology == "illumina" or technology is None:
                found = True
                id = name.split("_")[0]
                logging.info(f"{path}/{name}:")
                location_direct = f"/seq/{id}"
                location_illumina = f"/seq/illumina/runs/{id[:2]}/{id}"
                with open("resolve_orphaned_files.sh", "a") as out:
                    if DataObject(f"{location_direct}/{name}").exists():
                        rm_or_keep(obj, DataObject(f"{location_direct}/{name}"), out)
                    elif DataObject(f"{location_illumina}/{name}").exists():
                        rm_or_keep(obj, DataObject(f"{location_illumina}/{name}"), out)
                    else:
                        logging.info(f"does not exist")
                        pos = ""
                        if Collection(location_direct).exists():
                            pos = "direct"
                        if Collection(location_illumina).exists():
                            if pos == "direct":
                                pos = "both"
                            else:
                                pos = "illumina"
                        if pos == "direct":
                            logging.info(f"moving to {location_direct}")
                            out.write(f"imv {path}/{name} {location_direct}/{name} # {name} not present, runfolder at {location_direct}\n")
                        elif pos == "illumina":
                            logging.info(f"moving to {location_illumina}")
                            out.write(f"imv {path}/{name} {location_illumina}/{name} # {name} not present, runfolder at {location_illumina}\n")
                        elif pos == "both":
                            logging.warning(f"Two run folders for run {id}")
                            out.write(f"# {name} not present, two possible runfolders for this run, {location_direct} and {location_illumina}\n")
                        else:
                            logging.info("no illumina runfolder for this run")
                            found = False
                if found:
                    continue

            for t in technologies:
                if t == technology:
                    if non_illumina(t, obj):
                        continue

            # if the file was not found, and there was no runfolder
            with open ("still_lost.log", "a") as lost:
                lost.write(f"A location for {path}/{name} has not been found")


if __name__ == "__main__":
    main()
    