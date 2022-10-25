from exporter.graph.crawler import GraphCrawler
from exporter.graph.link_set import LinkSet
from exporter.graph.node_set import MetadataNodeSet


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

    @staticmethod
    def from_supplementary_files_info(supplementary_files_info, project):
        graph = ExperimentGraph()
        graph.nodes.add_nodes(supplementary_files_info.files + [project])
        suppl_files_link = GraphCrawler.supplementary_file_link_for(supplementary_files_info)
        graph.links.add_link(suppl_files_link)