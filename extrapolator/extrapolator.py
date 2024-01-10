from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime
import json
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

from tqdm import tqdm

from bgpy.as_graphs import CAIDAASGraphConstructor
from bgpy.bgpc import extrapolate

from mrt_collector import MRTCollector
from mrt_collector.mrt_collector import get_vantage_point_json


class Extrapolator:
    def __init__(self, max_block_size: int = 1000):
        self.max_block_size: int = max_block_size
        self.temp_dir: Path = Path(TemporaryDirectory().name)

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
        top_vantage_points = self._get_top_vantage_points(collector, 10)
        # Get the unique intersecting set of prefix IDs at each top vantage point
        # that don't have path poisoning
        joint_prefix_ids = set(self._get_top_vantage_points_prefix_ids(
            collector, top_vantage_points
        ))
        non_stub_asns, caida_tsv_path = self._get_non_stub_asns_and_caida_path(
            top_vantage_points
        )

        dirs = self._get_relevant_dirs(collector)
        max_block_id = self._get_max_block_id(dirs)
        for top_vantage_point in top_vantage_points:
            top_vantage_point_asn = top_vantage_point["asn"]
            for block_id in range(max_block_id + 1):
                tsv_paths = self._get_tsv_paths_for_block_id(dirs, block_id)
                out_path = self._get_block_id_guess_path(top_vantage_point_asn, block_id)
                extrapolate(
                    tsv_paths=[str(x) for x in tsv_paths],
                    origin_only_seeding=True,
                    valid_seed_asns=non_stub_asns,
                    omitted_vantage_point_asns=set([top_vantage_point_asn]),
                    valid_prefix_ids=joint_prefix_ids,
                    max_prefix_block_id=self.max_block_size,
                    output_asns=set([top_vantage_point_asn]),
                    out_path=str(out_path),
                    non_default_asn_cls_str_dict=dict(),
                    caida_tsv_path=str(caida_tsv_path),
                )
                raise NotImplementedError("modify and use local_ribs_to_tsv_func")
            raise NotImplementedError("Concatenate with header")
            raise NotImplementedError("multiprocess with limited cores")
            print("go do pw rn while this is running")
            raise NotImplementedError("calculate levenshtein distance")
        raise NotImplementedError("statistical significance")

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

    def _get_relevant_dirs(self, collector: MRTCollector) -> list[Path]:
        """Gets all MRT directories"""

        mrt_files = collector.get_mrt_files()
        mrt_files = tuple([x for x in mrt_files if x.unique_prefixes_path.exists()])
        return [x.formatted_dir for x in mrt_files]

    def _get_max_block_id(self, dirs: list[Path]) -> int:
        """Input is MRT directories. Output is max file number"""

        max_block_id = 0
        for dir_ in dirs:
            count = len(list((dir_ / str(self.max_block_size)).glob("*.tsv")))
            if count > max_block_id:
                max_block_id = count
        return max_block_id

    def _get_tsv_paths_for_block_id(
        self, dirs: list[Path], block_id: int
    ) -> list[Path]:
        """Returns TSV paths for a given block ID"""

        paths = list()
        for dir_ in dirs:
            path = dir_ / str(self.max_block_size) / f"{block_id}.tsv"
            assert path.exists(), path
            paths.append(path)
        return paths

    def _get_block_id_guess_path(self, vantage_point: int, block_id: int) -> Path:
        path = self.temp_dir / str(vantage_point) / "guess" / "temp" / f"{block_id}.tsv"
        path.parent.mkdir(exist_ok=True, parents=True)
        return path

    def _get_non_stub_asns_and_caida_path(
        self, top_vantage_points: list[int]
    ) -> tuple[set[int], Path]:
        """Returns non stub ASNs from CAIDA graph for use in extrapolation"""

        print("Getting non stub ASNs from AS graph")
        tsv_path = Path.home() / "Desktop" / "no_stub_caida.tsv"
        bgp_dag = CAIDAASGraphConstructor(tsv_path=tsv_path, stubs=False).run()
        print("Got non stub asns from AS Graph")
        # No stubs are left in the graph at this point
        non_stub_asns = set([as_obj.asn for as_obj in bgp_dag])
        msg = "Removed vantage point from the graph, this will break a lot"
        assert all(x in non_stub_asns for x in top_vantage_points), msg
        return non_stub_asns, tsv_path
