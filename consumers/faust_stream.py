"""Defines trends calculations for stations"""
import logging

import faust


logger = logging.getLogger(__name__)


# Faust will ingest records from Kafka in this format
class Station(faust.Record):
    stop_id: int
    direction_id: str
    stop_name: str
    station_name: str
    station_descriptive_name: str
    station_id: int
    order: int
    red: bool
    blue: bool
    green: bool


# Faust will produce records to Kafka in this format
class TransformedStation(faust.Record):
    station_id: int
    station_name: str
    order: int
    line: str


app = faust.App("stations-stream", broker="kafka://localhost:9092", store="memory://")
topic = app.topic("cta.traffic.stations", value_type=Station)
out_topic = app.topic("cta.traffic.stations.transformed", value_type=TransformedStation, partitions=1)
table = app.Table(
    "stations.transformation.table",
    default=int,
    partitions=1,
    changelog_topic=out_topic,
)


@app.agent(topic)
async def transformed_station(station_events):
    async for event in station_events:
        if event.red:
            color = "red"
        if event.blue:
            color = "blue"
        if event.green:
            color = "green"
        new_event = TransformedStation(
            station_id=event.station_id,
            station_name=event.station_name,
            order=event.order,
            line=color
        )
        await out_topic.send(value=new_event)


if __name__ == "__main__":
    app.main()
