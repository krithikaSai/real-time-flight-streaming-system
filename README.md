# Real-Time Flight Monitoring & Analytics System

Aviation data is highly fragmented across multiple independent sources. Real-time telemetry, flight schedules, airport details, and weather data are all published separately, and no single API provides a unified or complete view of global aviation activity. This creates gaps in
operational monitoring, delay analysis, and situational awareness. To understand real-time flight patterns and identify performance trends, an integrated system is required to collect, combine, and analyze data from multiple providers.

I have built a live dashboard containing global flight activity, operational metrics, delay analysis, and weather correlations, with the data collected from four external API's, using Python-based Kafka producers and consumers. 

### System Architecture

<img src="images/1.png" width="500"/>

### Data provided by each API

OpenSky – live flight telemetry <br>
AviationStack – flight schedules, statuses, and delays <br>
OpenWeather – airport-specific weather conditions <br>
AirLabs – global airport database with coordinates <br>


---
# Real Time Global Flights Dashboard

This section provides a high-level view of global air traffic using real-time telemetry and route metadata.

##SECTION 1 - GLOBAL OVERVIEW

### *Arc Map — Global Flight Routes*
Displays live flight routes across the world using aircraft positions from telemetry_events.Shows large-scale movement patterns and active corridors.

<img src="images/2.png" width="600"/>

### *Global Air Traffic Intensity Heatmap*
Aggregates flight positions into a density heatmap, highlighting busy regions and hubs.

<img src="images/3.png" width="600"/>

### *Top Origin and Destination Airports*
Ranks airports by number of departures and arrivals, based on flight_metadata, to identify the major hubs.

<img src="images/4.png" width="600"/>

### *Global Routes Reference Table*
Lists structured routes with airport coordinates and flight metadata, providing a searchable lookup.

<img src="images/5.png" width="500"/>

---

##SECTION 2 - LIVE OPERATIONAL METRICS

This section analyzes active flights in real time using telemetry data.

### *Flight Speed Distribution*

Histogram showing the distribution of aircraft velocities captured in the latest telemetry window.

<img src="images/6.png" width="500"/>

### *Flight State Distribution (Climb / Level / Descend)*
Classification of flights using vertical rate from telemetry_events, indicating their operational phase.

<img src="images/7.png" width="450"/>

### *Altitude vs Velocity Scatterplot*
Correlates altitude with speed to highlight climb profiles and performance patterns.

<img src="images/8.png" width="450"/>

### *Heading Distribution*
Shows the directional flow of global traffic using aircraft heading values.

<img src="images/9.png" width="300"/>

---

##SECTION 3 - FLIGHT PERFORMANCE AND DELAYS

This section uses structured metadata to analyze delay patterns and operational punctuality.

### *Average Delay per Airline*

Computes mean delay values per airline, offering comparative punctuality insights.

<img src="images/10.png" width="500"/>

### *Delay Distribution*

Histogram showing how delays spread across all tracked flights.

<img src="images/11.png" width="500"/>

### *Flight Status Breakdown*

Pie chart dividing flights into scheduled, active, and landed categories.

<img src="images/12.png" width="200"/>

### *Top Delayed Routes*

Lists the routes sorted by highest average delay.

<img src="images/13.png" width="550"/>

### *On-Time Performance Score*

Percentage of flights with delay under 15 minutes, following industry standards.

<img src="images/14.png" width="160"/>

---

##SECTION 4 - WEATHER FLIGHT CORRELATION

This section analyzes how weather conditions influence flight delays.

### *Temperature vs Time (Per Airport)*

Line chart showing temperature changes for active airports over time.

<img src="images/15.png" width="450"/>

### *Delay vs Temperature Scatterplot*

Displays correlation between temperature and flight delays for airport departures.

<img src="images/16.png" width="300"/>

### *Delay vs Wind Speed Scatterplot*

Explores how wind speed relates to delay patterns

<img src="images/17.png" width="300"/>

### *Weather Reference Table*

Raw weather measurements used across all correlation charts.

<img src="images/18.png" width="500"/>

---

##SECTION 5 - GEO VISUALS

These visualizations provide spatial context using maps and 3D geospatial layers.

### *Airport Locations Map*

Shows global airport coordinates from airport_data.

<img src="images/19.png" width="600"/>

### *Airline-Specific Arc Map*

Highlights the route network for a selected airline.

<img src="images/20.png" width="600"/>

### *India Route Intensity Heatmap*

Regional heatmap focused on Indian airspace activity

<img src="images/21.png" width="400"/>





