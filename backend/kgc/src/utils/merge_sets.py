"""Merge overlapping sets using graph-based connected component detection."""

from tqdm import tqdm


def merge_sets(sets: list[set[str]]) -> list[set[str]]:
    """Merge a list of sets with overlapping elements. Complexity: O(n^2 * m)."""

    def _get_islands(graph: dict[int, set[int]]) -> list[set[int]]:
        visited: set[int] = set()
        islands: list[set[int]] = []

        def _dfs(node: int, island: set[int]) -> None:
            visited.add(node)
            island.add(node)
            for neighbor in graph[node]:
                if neighbor not in visited:
                    _dfs(neighbor, island)

        for node in graph:
            if node not in visited:
                island: set[int] = set()
                _dfs(node, island)
                islands.append(island)

        return islands

    graph: dict[int, set[int]] = {i: set() for i in range(len(sets))}
    for i in tqdm(range(len(sets)), total=len(sets)):
        for j in range(i + 1, len(sets)):
            if sets[i] & sets[j]:
                graph[i].add(j)
                graph[j].add(i)

    islands = _get_islands(graph)

    merged: list[set[str]] = []
    for island in islands:
        merged_set: set[str] = set()
        for node in island:
            merged_set |= sets[node]
        merged.append(merged_set)

    return merged
