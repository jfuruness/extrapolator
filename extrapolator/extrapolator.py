from concurrent.futures import ProcessPoolExecutor, as_completed
import csv
from datetime import datetime
import json
from pathlib import Path
from pprint import pprint
import shutil
from statistics import mean
from tempfile import TemporaryDirectory
from typing import Any

import Levenshtein
from tqdm import tqdm, trange

from bgpy.as_graphs import CAIDAASGraphConstructor
from bgpy.bgpc import extrapolate

from mrt_collector import MRTCollector
from mrt_collector.mrt_collector import get_vantage_point_json

class Experiment(ABC):
    @property
    @abstractmethod
    def name(self) -> str:
        raise NotImplementedError

    @abstractmethod
    def change_kwargs(self, kwargs) -> None:
        raise NotImplementedError

class OriginOnlyExperiment(Experiment):
    name = "origin_only"

    def change_kwargs(self, kwargs) -> None:
        pass

class SeedingAlongPathExperiment(Experiment):
    name = "seeding_along_as_path"

    def change_kwargs(self, kwargs) -> None:
        kwargs["origin_only_seeding"] = False


class SeedingAlongPathWROVExperiment(Experiment):
    name = "seeding_along_as_path_w_rov"

    def change_kwargs(self, kwargs) -> None:
        kwargs["origin_only_seeding"] = False
        raise NotImplementedError("Non default asn cls dict")


class SeedingAlongPathWROVAndMHProviderPrefExperiment(Experiment):
    name = "seeding_along_as_path_w_rov_and_mh_provider_pre"

    def change_kwargs(self, kwargs) -> None:
        kwargs["origin_only_seeding"] = False
        raise NotImplementedError("Non default asn cls dict")
        raise NotImplementedError("mh provider pref")


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
        non_stub_asns, caida_tsv_path, relationships = self._get_non_stub_asns_and_caida_path(
            top_vantage_points
        )

        dirs = self._get_relevant_dirs(collector)
        max_block_id = self._get_max_block_id(dirs)
        stats = dict()
        for experiment in (
            OriginOnlyExperiment(),
            SeedAlongPathExperiment(),
            SeedingAlongPathWROVExperiment(),
            SeedingAlongPathWROVAndMHProviderPrefExperiment()
        ):
            stats[experiment.name] = dict()
            for top_vantage_point in top_vantage_points:
                top_vantage_point_asn = top_vantage_point["asn"]
                levenshtein_distances = list()
                ground_truth_path_lens = list()
                block_ids = set()
                for block_id in [max_block_id - 1000]:#trange(max_block_id + 1):
                    tsv_paths = self._get_tsv_paths_for_block_id(dirs, block_id)
                    out_path = self._get_block_id_guess_path(
                        top_vantage_point_asn, block_id
                    )
                    extrapolate(
                        tsv_paths=[str(x) for x in tsv_paths],
                        origin_only_seeding=False,
                        valid_seed_asns=non_stub_asns,
                        omitted_vantage_point_asns=set([top_vantage_point_asn]),
                        valid_prefix_ids=joint_prefix_ids,
                        max_prefix_block_id=self.max_block_size,
                        output_asns=set([top_vantage_point_asn]),
                        out_path=str(out_path),
                        non_default_asn_cls_str_dict=dict(),
                        caida_tsv_path=str(caida_tsv_path),
                    )
                    block_ids.add(block_id)
                guess_agg_path = self._concatenate_block_id_guesses(
                    top_vantage_point_asn, max_block_id
                )
                gt_agg_path = self._get_vantage_point_gt(top_vantage_point_asn, collector)
                l_distances, gt_path_lens = self._get_levenshtein_dist(
                    guess_agg_path, gt_agg_path, joint_prefix_ids, block_ids
                )
                levenshtein_distances.extend(l_distances)
                ground_truth_path_lens.extend(gt_path_lens)
                from math import sqrt
                from statistics import stdev
                def get_yerr(trial_data):
                    if len(trial_data) > 1:
                        yerr_num = 1.645 * 2 * stdev(trial_data)
                        yerr_denom = sqrt(len(trial_data))
                        return float(yerr_num / yerr_denom)
                    else:
                        return 0

                stats[experiment.name][top_vantage_point_asn] = {
                    "avg_l_dist": mean(levenshtein_distances),
                    "avg_l_dist_yerr": get_yerr(levenshtein_distances),
                    "avg_as_path_len": mean(ground_truth_path_lens)
                    "avg_as_path_len_yerr": get_yerr(ground_truth_path_lens)
                }
                pprint(stats)
        raise NotImplementedError("statistical significance")

        raise NotImplementedError("add to ppt the levenshtein distance")
        raise NotImplementedError("Must deal with seeding conflicts")

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
            as_rank = [x for x in top_vantage_points
                       if int(x["asn"]) == int(vantage_point)][0]["as_rank"]
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

    def _get_vantage_point_guess_path(self, vantage_point: int) -> Path:
        path = self.temp_dir / str(vantage_point) / "guess" / "agg.tsv"
        path.parent.mkdir(exist_ok=True, parents=True)
        return path

    def _get_vantage_point_gt_path(self, vantage_point: int) -> Path:
        path = self.temp_dir / str(vantage_point) / "gt" / "agg.tsv"
        path.parent.mkdir(exist_ok=True, parents=True)
        return path

    def _get_vantage_point_gt_temp_path(self, vantage_point: int) -> Path:
        path = self.temp_dir / str(vantage_point) / "gt" / "temp"
        path.mkdir(exist_ok=True, parents=True)
        return path

    def _get_non_stub_asns_and_caida_path(
        self, top_vantage_points: list[int]
    ) -> tuple[set[int], Path]:
        """Returns non stub ASNs from CAIDA graph for use in extrapolation"""

        print("Getting non stub ASNs from AS graph")
        tsv_path = Path.home() / "Desktop" / "no_stub_caida.tsv"
        bgp_dag = CAIDAASGraphConstructor(
            as_graph_collector_kwargs = {
                "dl_time": datetime(2023, 12, 12, 0, 0, 0),
            },
            tsv_path=tsv_path,
            stubs=True
        ).run()

        relationships = dict()
        for as_obj in bgp_dag:
            relationships[as_obj.asn] = dict()
            for provider_obj in as_obj.providers:
                relationships[as_obj.asn][provider_obj.asn] = Relationships.PROVIDERS.value
            for customer_obj in as_obj.customers:
                relationships[as_obj.asn][customer_obj.asn] = Relationships.CUSTOMERS.value
            for peer_obj in as_obj.peers:
                relationships[as_obj.asn][peer_obj.asn] = Relationships.PEERS.value

        print("Got non stub asns from AS Graph")
        # No stubs are left in the graph at this point
        non_stub_asns = set([as_obj.asn for as_obj in bgp_dag])
        msg = "Removed vantage point from the graph, this will break a lot"
        assert all(x in non_stub_asns for x in top_vantage_points), msg
        return non_stub_asns, tsv_path, relationships

    def _concatenate_block_id_guesses(
        self, vantage_point_asn: int, max_block_id: int
    ) -> Path:

        agg_path = self._get_vantage_point_guess_path(vantage_point_asn)
        with agg_path.open("w") as agg_f:
            agg_f.write(
                "dumping_asn\t"
                "prefix\t"
                "as_path\t"
                "timestamp\t"
                "seed_asn\t"
                "roa_valid_length\t"
                "roa_origin\t"
                "recv_relationship\t"
                "withdraw\t"
                "traceback_end\t"
                "communities\n"
            )

            block_id_paths = [self._get_block_id_guess_path(vantage_point_asn, block_id)
                              for block_id in range(max_block_id + 1)]
            block_id_paths = [x for x in block_id_paths if x.exists()]

            for path in block_id_paths:
                with path.open() as f:
                    shutil.copyfileobj(f, agg_f)
        return agg_path

    def _get_vantage_point_gt(
        self, vantage_point: int, collector: MRTCollector
    ) -> Path:

        mrt_files = collector.get_mrt_files()
        vantage_points_to_dirs = collector.get_vantage_points_to_dirs(
            mrt_files, self.max_block_size
        )

        tmp_dir: Path = self._get_vantage_point_gt_temp_path(vantage_point)
        agg_path: Path = self._get_vantage_point_gt_path(vantage_point)
        agg_path.unlink(missing_ok=True)
        dirs = vantage_points_to_dirs[vantage_point]
        assert dirs

        # Get header
        for dir_ in dirs:
            for formatted_path in (Path(dir_) / str(self.max_block_size)).glob("*.tsv"):
                with formatted_path.open() as f:
                    with agg_path.open("w") as agg_path_f:
                        for line in f:
                            agg_path_f.write(line)
                            break

        from subprocess import check_call
        # Use awk to write temporary files containing the vantage point
        for dir_ in dirs:
            print(f"using awk on {dir_}")
            cmd = ("""find "$ROOT_DIRECTORY" -name '*.tsv' | """
                   # NOTE: using just 1 core for this
                   # since we are multiprocessing elsewhere
                   """xargs -P 1 -I {} """
                   """sh -c 'awk -F"\t" "\$3 ~ /^asn($| )/" {} """  # noqa
                   """> '"$TMP_DIR"'"/"""
                   """$(echo {} | md5sum | cut -d" " -f1).tmp"'""")
            cmd = cmd.replace("$ROOT_DIRECTORY", str(dir_))
            cmd = cmd.replace("asn", str(vantage_point))
            cmd = cmd.replace("$TMP_DIR", str(tmp_dir))
            check_call(cmd, shell=True)
        print("combining files")
        # Combine the files
        # Get only unique lines or else it will screw up the counts
        # uniq too slow https://unix.stackexchange.com/a/581324/477240
        # check_call(f"cat {tmp_dir}/*.tmp | sort | uniq >> {agg_path}", shell=True)
        check_call(f"cat {tmp_dir}/*.tmp | awk '!x[$0]++' >> {agg_path}", shell=True)
        print("combined files")
        return agg_path

    def _get_levenshtein_dist(
        self,
        guess_agg_path: Path,
        gt_agg_path: Path,
        joint_prefix_ids: set[int],
        block_ids: set[int]
    ) -> float:
        """Returns levenshtein distance"""

        if not isinstance(joint_prefix_ids, set):
            raise TypeError("Not a set")

        def get_as_path(dct):
            return tuple([int(x) for x in dct["as_path"].split()])

        guess_prefix_to_as_path = dict()
        # input(guess_agg_path)
        with guess_agg_path.open() as f:
            for row in csv.DictReader(f, delimiter="\t"):
                guess_prefix_to_as_path[row["prefix"]] = get_as_path(row)

        distances = list()
        gt_path_lens = list()
        # input(gt_agg_path)
        with gt_agg_path.open() as f:
            for row in csv.DictReader(f, delimiter="\t"):
                if (
                    int(row["prefix_id"]) in joint_prefix_ids
                    and int(row["block_id"]) in block_ids
                ):
                    guess_as_path = guess_prefix_to_as_path.get(row["prefix"], ())
                    if guess_as_path == ():
                        print(f"empty for {row['prefix']}")
                    gt_as_path = get_as_path(row)
                    gt_path_lens.append(len(gt_as_path))
                    # Setting the score cutoff speeds up levenshtein
                    score_cutoff = max(len(guess_as_path), len(gt_as_path))
                    # print(gt_as_path)
                    # print(guess_as_path)
                    # input()
                    distances.append(
                        Levenshtein.distance(
                            guess_as_path,
                            gt_as_path,
                            score_cutoff=score_cutoff)
                    )
        return distances, gt_path_lens
