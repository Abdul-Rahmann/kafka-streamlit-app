import faust

# Faust app
app = faust.App(
    'activity-analytics-app',
    broker='kafka://localhost:9092',
    value_serializer='json'
)

# Input topic (raw events)
raw_topic = app.topic('user-activity')

# Output topic (aggregated results)
agg_topic = app.topic('user-activity-aggregates')

# Table to store counts per user
user_counts = app.Table(
    'user_event_counts',
    default=int
)

# Windowed table (10-second tumbling windows)
windowed_counts = app.Table(
    'user_windowed_counts',
    default=int
).tumbling(10.0)


@app.agent(raw_topic)
async def process(stream):
    async for event in stream:
        user = event["user"]

        # global running count
        user_counts[user] += 1

        # windowed count
        windowed_counts[user] += 1

        # send updated aggregates
        result = {
            "user": user,
            "total_events": user_counts[user],
            "events_last_10_sec": windowed_counts[user].now()
        }

        await agg_topic.send(value=result)
        print("Published aggregate:", result)


if __name__ == "__main__":
    app.main()