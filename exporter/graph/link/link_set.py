from typing import Union, Dict, List

from .process import ProcessLink
from .supplementary_files import SupplementaryFilesLink

Link = Union[ProcessLink, SupplementaryFilesLink]


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
