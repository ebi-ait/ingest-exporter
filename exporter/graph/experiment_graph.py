from copy import deepcopy
from dataclasses import dataclass
from typing import List, Dict, Iterable, Any, Union

from exporter.graph.process_link import ProcessLink
from exporter.metadata.resource import MetadataResource


@dataclass
class SupplementedEntity:
    entity_type: str
    entity_id: str

    def to_dict(self) -> Dict[str, str]:
        return dict(
            entity_type=self.entity_type,
            entity_id=self.entity_id
        )


@dataclass
class SupplementaryFile:
    file_type: str
    file_id: str

    def to_dict(self) -> Dict[str, str]:
        return dict(
            file_type=self.file_type,
            file_id=self.file_id
        )


@dataclass
class SupplementaryFileLink:
    supplemented_entity: SupplementedEntity
    files: Iterable[SupplementaryFile]

    def to_dict(self) -> Dict[str, Any]:
        return dict(
            link_type="supplementary_file_link",
            entity=self.supplemented_entity.to_dict(),
            files=[file.to_dict() for file in self.files]
        )


Link = Union[ProcessLink, SupplementaryFileLink]


class LinkSet:

    def __init__(self):
        self.links: Dict[str, Link] = dict()

    def add_links(self, links: List[Link]):
        for link in links:
            self.add_link(link)

    def add_link(self, link: Link):
        if isinstance(link, ProcessLink):
            link_uuid = link.process_uuid
        else:
            link_uuid = link.supplemented_entity.entity_id

        self.links[link_uuid] = link

    def get_links(self) -> List[Link]:
        return list(self.links.values())

    def to_dict(self):
        return dict(
            links=[link.to_dict() for link in self.get_links()]
        )


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
