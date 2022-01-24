#!/usr/bin/env python3

import logging
import structlog

logging.basicConfig(level=logging.INFO, filename="orphan_files.log", encoding="utf-8")

structlog.configure(
    logger_factory=structlog.stdlib.LoggerFactory()
)

log = structlog.get_logger(__file__)

from partisan.irods import (
    Collection,
    DataObject,
    AVU,
    query_metadata
)


object_file = "objects"
resolve = "resolve_orphaned_files.sh"
keptfiles = "kept_files"


def rm_or_keep(landf, existing, out, kept):
    log.info(f"exists at {existing}")
    if existing.checksum() == landf.checksum():
        log.info(f"has correct checksum: {landf.checksum()} == {existing.checksum()}")
        if AVU("md5", existing.checksum()) in existing.metadata():
            log.info(f"has correct checksum metadata")
            out.write(f"irm {landf.path}/{landf.name} # md5 ok, md5 meta ok, exists as {existing.path}/{existing.name}\n")
        else:
            log.warning(f"does not have correct checksum metadata")
            kept.write(f"{landf.path}/{landf.name} kept due to incorrect md5 metadata on the current file\n")
    else:
        log.warning(f"has incorrect checksum: {landf.checksum()} != {existing.checksum()}")
        kept.write(f"{landf.path}/{landf.name} kept due to difference in checksum compared with the current file\n")


def main():
    lost_and_found = Collection("/seq/lostandfound")
    # check for premade object list
    objects = []
    premade = False
    try:
        with open("objects", "r") as object_list:
            objects = [DataObject(obj) for obj in object_list.readlines()]
        premade = True
    except FileNotFoundError:
        objects = lost_and_found.iter_contents()
    with open(resolve, "a") as out, open(keptfiles, "a") as kept:
        for obj in objects:
            if type(obj) == DataObject:
                path = obj.path
                name = obj.name
                if not premade:
                    # compile list of data objects to avoid rerunning recursive contents
                    with open("objects", "a") as object_list:
                        object_list.write(f"{path}/{name}\n")
                log.info(f"{path}/{name}:")
                found = False
                # path may be exactly correct except for lost and found structure
                try:
                    split_path = str(path).split("/")[4:]
                    actual_obj = DataObject(f"/seq/{'/'.join(split_path)}/{name}")
                    if actual_obj.exists():
                        rm_or_keep(obj, actual_obj, out, kept)
                        found = True
                    else:
                        coll = Collection(f"/seq/{'/'.join(split_path)}")
                        if coll.exists():
                            log.info(f"moving to {coll.path}/{name}")
                            out.write(f"imv {path}/{name} {coll}/{name} # {name} not present, collection at {coll}\n")
                            found = True
                            break
                except IndexError:
                    pass  # some objects are not as deep in collections as others
                if found:
                    continue

                found = True
                runid = name.split("_")[0]
                if runid[0].isdigit():
                    location_direct = f"/seq/{runid}"
                    location_illumina = f"/seq/illumina/runs/{runid[:2]}/{runid}"
                    if DataObject(f"{location_direct}/{name}").exists():
                        rm_or_keep(obj, DataObject(f"{location_direct}/{name}"), out, kept)
                    elif DataObject(f"{location_illumina}/{name}").exists():
                        rm_or_keep(obj, DataObject(f"{location_illumina}/{name}"), out, kept)
                    else:
                        log.info(f"does not exist")
                        pos = ""
                        if Collection(location_direct).exists():
                            pos = "direct"
                        if Collection(location_illumina).exists():
                            if pos == "direct":
                                pos = "both"
                            else:
                                pos = "illumina"
                        if pos == "direct":
                            log.info(f"moving to {location_direct}")
                            out.write(f"imv {path}/{name} {location_direct}/{name} # {name} not present, runfolder at "
                                      f"{location_direct}\n")
                        elif pos == "illumina":
                            log.info(f"moving to {location_illumina}")
                            out.write(f"imv {path}/{name} {location_illumina}/{name} # {name} not present, runfolder at"
                                      f" {location_illumina}\n")
                        elif pos == "both":
                            log.warning(f"Two run folders for run {runid}")
                            kept.write(f"{path}/{name} kept, two possible runfolders for this run, {location_direct} "
                                       f"and {location_illumina}\n")
                        else:
                            log.info("no illumina runfolder for this run")
                            found = False
                if found:
                    continue

                # find objects with the same md5
                log.info("searching by md5 metadata")
                matches = query_metadata(AVU("md5", obj.checksum()), zone='/seq', collection=False)
                complete_matches = []
                for match in matches:
                    if match.name == obj.name:
                        log.info(f"{match.path}/{match.name} has correct name")
                        if match.checksum() == obj.checksum():
                            log.info("match has correct checksum")
                            complete_matches.append(match)
                        else:
                            log.info("match has incorrect checksum")
                if len(complete_matches) > 0:
                    log.info(f"At least one complete match, file can be regenerated if needed elsewhere")
                    out.write(f"irm {path}/{name} # md5 ok, md5 meta ok, exists as {complete_matches[0].path}/"
                              f"{complete_matches[0].name}\n")
                    found = True

                else:
                    log.info(f"No complete matches, keeping file")
                    kept.write(f"{path}/{name} kept because a location has not been found\n")


if __name__ == "__main__":
    main()
    