## Kafka, Stream Processing - US Flights (flink)

```dtd 
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