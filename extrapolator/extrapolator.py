from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime
import json
from tqdm import tqdm
from typing import Any

from mrt_collector import MRTCollector
from mrt_collector.mrt_collector import get_vantage_point_json


class Extrapolator:
    def __init__(self, max_block_size: int = 1000):
        self.max_block_size: int = max_block_size

    def run(self):
        """See steps below

        1. Run the MRT Collector
            a. This downloads ROV info, hijack info, etc
            b. This downloads all MRT RIB dumps and formats them for extrapolation
        2.
        """

        collector = MRTCollector(dl_time=datetime(2023, 12, 12, 0, 0, 0), cpus=1)
        # This downloads ROV info, hijack info, etc
        # This downloads all MRT RIB dumps and formats them for extrapolation
        # This gets vantage point statistics
        collector.run(max_block_size=self.max_block_size)
        # Get the top 10 vantage points to use
        # These must have over 800k prefixes, have num_ans == num_prefixes, and
        # we sort by AS rank
        top_vantage_points, relevant_paths = self._get_top_vantage_points(collector, 10)
        # Get the unique intersecting set of prefix IDs at each top vantage point
        # that don't have path poisoning
        joint_prefix_ids = self._get_top_vantage_points_prefix_ids(
            collector, top_vantage_points
        )
        relevant_paths = self.get_relevant_paths(collector)
        for top_vantage_point in top_vantage_points:
            """
                    //input: vector of dir strings, max_block_id, max_prefix_block_id, prefix_ids (set), vantage_point_asn
                    //for each block id in max_block_id:
                    //  announcements = vector()
                    //  for each dir in dir_strings:
                    //    get announcements from dir/block_id
                    //  final_announcements = vector()
                    //  for announcement in announcements:
                    //    if announcement.prefix_id (NOT prefix_block_id) in prefix_ids:
                    //      final_announcements.push_back(prefix_block_id)
                    //  engine = Engine()
                    //  engine.setup(final_announcements)
                    //  engine.run()
                    //  THIS MUST APPEND!!! Can't erase anything!!!
                    //  engine.local_ribs_to_csv([vantage_point_asn])
            """
            raise NotImplementedError(
                "for each vantage point, "
                "ingest anns"
                "   only for the relevant prefix ids"
                "   and excluding any where the vantage point is the first ASN in the path"
                "propagate"
                "   output only the vantage point ASN"
            )
            print("go do pw rn while this is running")
            raise NotImplementedError("calculate levenshtein distance")
        raise NotImplementedError("statistical significance")

        raise NotImplementedError("add top vantage point data to appropriate ppt slide")
        raise NotImplementedError("add to ppt the levenshtein distance")
        raise NotImplementedError("Must deal with seeding conflicts")
        raise NotImplementedError(
            "remove stubs other than vantage point ASN assuming seeing along path is better"
            "and for origin only seeding, if the stub is missing seed it one higher"
        )

        raise NotImplementedError("run for seeding along as path")
        raise NotImplementedError("run for rov + seeding along the as path")
        # raise NotImplementedError("if ROV doesn't matter, remove stubs?")
        raise NotImplementedError("run for multihomed provider preference")
        raise NotImplementedError("add to ppt, go through docs")
        raise NotImplementedError("prob link?")

    def _get_top_vantage_points(
        self, collector: MRTCollector, max_vantage_points: int = 10
    ) -> list[dict[str, Any]]:
        top_vantage_points = list()
        vantage_point_stats_path = collector.analysis_dir / "vantage_point_stats.json"
        with vantage_point_stats_path.open() as f:
            data = json.load(f)
            sorted_vantage_points = list(
                sorted(data.values(), key=lambda x: x["as_rank"])
            )
            for vantage_point in sorted_vantage_points:
                if vantage_point["num_prefixes"] < 800_000:
                    continue
                elif vantage_point["num_anns"] > 1.5 * vantage_point["num_prefixes"]:
                    continue

                if vantage_point["num_anns"] == vantage_point["num_prefixes"]:
                    top_vantage_points.append(vantage_point)
                    if len(top_vantage_points) >= max_vantage_points:
                        break
        return top_vantage_points

    def _get_top_vantage_points_prefix_ids(
        self, collector: MRTCollector, top_vantage_points: list[dict[str, Any]]
    ) -> tuple[int]:
        """Gets the unique set of non path poisoning prefixes at each vantage point"""

        # Get the vantage points to dirs for top vantage points only
        mrt_files = collector.get_mrt_files()
        vantage_points_to_dirs = collector.get_vantage_points_to_dirs(
            mrt_files, self.max_block_size
        )
        top_vantage_point_asns = set([int(v["asn"]) for v in top_vantage_points])
        vantage_points_to_dirs = {
            int(k): v
            for k, v in vantage_points_to_dirs.items()
            if int(k) in top_vantage_point_asns
        }
        assert vantage_points_to_dirs

        # Load up old stats
        stat_path = collector.analysis_dir / "top_vantage_point_stats.json"
        try:
            with stat_path.open() as f:
                data = {int(k): v for k, v in json.load(f).items()}
        except FileNotFoundError:
            with stat_path.open("w") as f:
                json.dump(dict(), f, indent=4)
            data = dict()
        except Exception as e:
            print(e)
            input("ensure that this exception is handled")

        iterable = list()
        for vantage_point, dirs in tqdm(
            vantage_points_to_dirs.items(),
            total=len(vantage_points_to_dirs),
            desc="getting stats for top vantage points",
        ):
            if int(vantage_point) in data:
                print(f"skipping {vantage_point} since it's already been parsed")
                continue
            as_rank = [x for x in top_vantage_points if int(x["asn"]) == int(vantage_point)][0]["as_rank"]
            get_path_poisoning = True
            iterable.append(
                [vantage_point, dirs, as_rank, self.max_block_size, get_path_poisoning]
            )

        func = get_vantage_point_json
        if False:
            for args in tqdm(
                iterable, total=len(iterable), desc="getting top vpoint stats"
            ):
                vantage_point_json = func(*args)
                try:
                    with stat_path.open() as f:
                        data = json.load(f)
                except Exception as e:
                    print(e)
                    data = dict()
                data[vantage_point_json["asn"]] = vantage_point_json
                with stat_path.open("w") as f:
                    json.dump(data, f, indent=4)

        else:
            # https://stackoverflow.com/a/63834834/8903959
            with ProcessPoolExecutor(max_workers=10) as executor:
                futures = [executor.submit(func, *x) for x in iterable]
                for future in tqdm(
                    as_completed(futures),
                    total=len(iterable),
                    desc="Getting top vantage point stats",
                ):
                    # reraise any exceptions from the processes
                    vantage_point_json = future.result()
                    try:
                        with stat_path.open() as f:
                            data = json.load(f)
                    except Exception as e:
                        print(e)
                        data = dict()
                    data[vantage_point_json["asn"]] = vantage_point_json
                    with stat_path.open("w") as f:
                        json.dump(data, f, indent=4)

        # Intersect and return all the prefixes
        with stat_path.open() as f:
            # Get prefix ids present at all vantage points
            joint_prefix_ids = set()
            for i, (asn, inner_dict) in enumerate(json.load(f).items()):
                if i == 0:
                    joint_prefix_ids = set(
                        inner_dict["no_path_poisoning_prefix_ids_set"]
                    )
                else:
                    joint_prefix_ids = joint_prefix_ids.intersection(
                        set(inner_dict["no_path_poisoning_prefix_ids_set"])
                    )
            return tuple(list(sorted(joint_prefix_ids)))

    def get_relevant_paths(self, collector: MRTCollector) -> list[str]:

        mrt_files = collector.get_mrt_files()
        dir_to_tsv_paths = dict()
        for mrt_file in mrt_files:
            dir_to_tsv_paths[str(mrt_file.formatted_dir)] = list()
            for formatted_path in (
                mrt_file.formatted_dir / str(max_block_size)
                    ).glob("*.tsv"):
                dir_to_tsv_paths[str(mrt_file.formatted_dir)].append(
                    str(formatted_path)
                )

        print("Getting relevant paths")
        return mrtc.get_relevant_paths(dir_to_tsv_paths)
