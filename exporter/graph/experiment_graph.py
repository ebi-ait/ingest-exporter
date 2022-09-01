from copy import deepcopy
from typing import List

from exporter.graph.link_set import LinkSet
from exporter.metadata.resource import MetadataResource


class MetadataNodeSet:

    def __init__(self):
        self.obj_uuids = set()
        self.objs = []

    def __contains__(self, item: MetadataResource):
        return item.uuid in self.obj_uuids

    def add_node(self, node: MetadataResource):
        if node.uuid in self.obj_uuids:
            pass
        else:
            self.obj_uuids.add(node.uuid)
            self.objs.append(node)

    def add_nodes(self, nodes: List[MetadataResource]):
        for node in nodes:
            self.add_node(node)

    def get_nodes(self) -> List[MetadataResource]:
        return [deepcopy(obj) for obj in self.objs]


class ExperimentGraph:
    links: LinkSet
    nodes: MetadataNodeSet

    def __init__(self):
        self.links = LinkSet()
        self.nodes = MetadataNodeSet()

    def extend(self, graph: 'ExperimentGraph'):
        for link in graph.links.get_links():
            self.links.add_link(link)

        for node in graph.nodes.get_nodes():
            self.nodes.add_node(node)

        return self
