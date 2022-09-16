from typing import Iterable, Set, List, Dict

from exporter.graph.input import Input
from exporter.graph.output import Output
from exporter.graph.protocol_link import ProtocolLink


class ProcessLink:
    def __init__(self, process_uuid: str, process_type: str,
                 inputs: Iterable[Input], outputs: Iterable[Output], protocols: Iterable[
                ProtocolLink]):
        self._input_uuids: Set[str] = set()
        self._outputs_uuids: Set[str] = set()
        self._protocol_uuids: Set[str] = set()

        self.process_uuid = process_uuid
        self.process_type = process_type
        self.inputs: List[Input] = list()
        self.outputs: List[Output] = list()
        self.protocols: List[ProtocolLink] = list()

        for i in inputs:
            self.add_input(i)

        for o in outputs:
            self.add_output(o)

        for p in protocols:
            self.add_protocol(p)

    def add_input(self, i: Input):
        if i.input_uuid not in self._input_uuids:
            self._input_uuids.add(i.input_uuid)
            self.inputs.append(i)

    def add_output(self, o: Output):
        if o.output_uuid not in self._outputs_uuids:
            self._outputs_uuids.add(o.output_uuid)
            self.outputs.append(o)

    def add_protocol(self, p: ProtocolLink):
        if p.protocol_uuid not in self._protocol_uuids:
            self._protocol_uuids.add(p.protocol_uuid)
            self.protocols.append(p)

    def to_dict(self) -> Dict:
        return dict(
            link_type="process_link",
            process_id=self.process_uuid,
            process_type=self.process_type,
            inputs=[i.to_dict() for i in self.inputs],
            outputs=[o.to_dict() for o in self.outputs],
            protocols=[p.to_dict() for p in self.protocols]
        )
