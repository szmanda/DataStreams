## Kafka, Stream Processing - US Flights (flink)

```
                          ┌─────────────────┐     ┌─────────────────────┐  
                          │ Airports        │     │ Flink               │  
                          │ CSV source      ├─────►                     │  
                          └─────────────────┘     │                     │  
                                                  │                     │  
  ┌─────────────────┐     ┌─────────────────┐     │                     │  
  │ Flights         │     │ flight-records  │     │                     │  
  │ CSV source      ├─────► Kafka topic     ├─────►                     │  
  └─────────────────┘     └─────────────────┘     └─────────────────────┘                                                                      
```