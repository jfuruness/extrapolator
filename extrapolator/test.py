from dataclasses import dataclass, field

@dataclass
class VantagePointStats:
    asn: int
    prefix_id_set: set[int] = field(default_factory=set)
    no_path_poisoning_prefix_id_set: set[int] = field(default_factory=set)
    ann_count: int = 0
    as_rank: int = 500000

    def __lt__(self, other):
        if isinstance(other, VantagePointStats):
            if self.as_rank < other.as_rank:
                return True
            elif self.as_rank > other.as_rank:
                return False
            else:
                if self.ann_count > other.ann_count:
                    return True
                elif self.ann_count < other.ann_count:
                    return False
                else:
                    return self.asn < other.asn


    def add_ann(self, prefix_id: int, path_poisoning: bool) -> None:
        self.prefix_id_set.add(prefix_id)
        self.ann_count += 1
        if not path_poisoning:
            self.no_path_poisoning_prefix_id_set.add(prefix_id)

    def to_json(self):
        return {
            "asn": self.asn,
            "as_rank": self.as_rank,
            "num_prefixes": len(self.prefix_id_set),
            "num_anns": self.ann_count,
            "no_path_poisoning_prefix_ids_set": list(self.no_path_poisoning_prefix_id_set)
        }


def get_vantage_point_stats(vantage_point: int, as_rank: int, file_paths: list[str]):
    stat = VantagePointStats(cur_vantage_point, as_rank=as_rank)
    for file_path in file_paths:
        with file_path.open() as f:
            for row in csv.DictReader(f, delimiter="\t"):
                # No AS sets
                if "}" in row["as_path"]:
                    continue
                as_path = [int(x) for x in row["as_path"].split()]
                vantage_point = as_path[0]
                if vantage_point != cur_vantage_point:
                    continue

                # If no path poisoning
                path_poisoning = True
                if (
                    row["invalid_as_path_asns"] == "[]"
                    # and row["ixps_in_as_path"] not in [None, "", "[]"]
                    and row["prepending"] == "False"
                    and row["as_path_loop"] == "False"
                    and row["input_clique_split"] == "False"
                ):
                    path_poisoning = False

                vantage_point_stats[vantage_point].add_ann(
                    row["prefix_id"], path_poisoning
                )
    return stat
