from __future__ import annotations

from exporter.metadata.node_set import MetadataNodeSet

from .entity.supplementary_file import SupplementaryFile
from .entity.supplemented_entity import SupplementedEntity
from .info.supplementary_files import SupplementaryFilesInfo
from .link.link_set import LinkSet
from .link.supplementary_file import SupplementaryFileLink


class ExperimentGraph:
    links: LinkSet
    nodes: MetadataNodeSet

    def __init__(self):
        self.links = LinkSet()
        self.nodes = MetadataNodeSet()

    def extend(self, graph: ExperimentGraph):
        for link in graph.links.get_links():
            self.links.add_link(link)

        for node in graph.nodes.get_nodes():
            self.nodes.add_node(node)

        return self

    @staticmethod
    def from_supplementary_files_info(supplementary_files_info, project):
        graph = ExperimentGraph()
        graph.nodes.add_nodes(supplementary_files_info.files + [project])
        suppl_files_link = ExperimentGraph.supplementary_file_link_for(supplementary_files_info)
        graph.links.add_link(suppl_files_link)
        return graph

    @staticmethod
    def supplementary_file_link_for(supplementary_files_info: SupplementaryFilesInfo) -> SupplementaryFileLink:
        for_entity = supplementary_files_info.for_entity
        supplementary_files = [SupplementaryFile(file.concrete_type(), file.uuid) for file in supplementary_files_info.files]

        return SupplementaryFileLink(
            SupplementedEntity(for_entity.concrete_type(), for_entity.uuid),
            supplementary_files
        )
