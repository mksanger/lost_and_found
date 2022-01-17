#!/usr/bin/env python3
from partisan.irods import (
    Collection,
    DataObject,
    AVU
)
import logging


def rm_or_keep(landf, existing, out):
    logging.info(f"exists at {existing}")
    if existing.checksum() == landf.checksum():
        logging.info(f"has correct checksum: {landf.checksum()} == {existing.checksum()}")
        if AVU("md5", existing.checksum()) in existing.metadata():
            logging.info(f"has correct checksum metadata")
            out.write(f"irm {landf.path} # md5 ok, md5 meta ok, exists as {existing.path}\n")
        else:
            logging.warning(f"does not have correct checksum metadata")
            out.write(f"# md5 ok, md5 meta NOT OK, exists as {existing.path}\n")
    else:
        logging.warning(f"has incorrect checksum: {landf.checksum()} != {existing.checksum()}")
        out.write(f"# md5 NOT OK, exists as {existing.path}\n")


def main():
    logging.basicConfig(filename='orphan_files.log', encoding='utf-8', level=logging.INFO)
    lost_and_found = Collection("/seq/lostandfound")

    with open("resolve_orphaned_files.sh", "w") as out:

        for obj in lost_and_found.contents(recurse=True):
            if type(obj) == DataObject:
                path = obj.path
                name = obj.name
                # simple case - assume files are illumina and begin with run id
                id = name.split("_")[0]
                logging.info(f"{path}/{name}:")
                location_direct = f"/seq/{id}/"
                location_illumina = f"/seq/illumina/runs/{id[:2]}/{id}/"
                if DataObject(f"{location_direct}{name}").exists():
                    rm_or_keep(obj, DataObject(f"{location_direct}{name}"), out)
                elif DataObject(f"{location_illumina}{name}").exists():
                    rm_or_keep(obj, DataObject(f"{location_illumina}{name}"), out)
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
                        logging.info(f"not present, moving to {location_direct}")
                        out.write(f"imv {path}/{name} {location_direct} # {name} not present, runfolder at {location_direct}\n")
                    elif pos == "illumina":
                        logging.info(f"not present, moving to {location_illumina}")
                        out.write(f"imv {path}/{name} {location_illumina} # {name} not present, runfolder at {location_illumina}\n")
                    elif pos == "both":
                        logging.warning(f"Two run folders for run {id}")
                        out.write(f"# {name} not present, two possible runfolders for this run, {location_direct} and {location_illumina}\n")
                    else:
                        logging.warning("not present, no runfolder for this run")
                        out.write(f"# {name} not present, no runfolder for this run")


if __name__ == "__main__":
    main()
    