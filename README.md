# SimpleDA - Simple Message Passing Simulator for Distributed Algorithms.


Time is modeled as an heap, allowing processes to:
- schedule some execution in the future (hence modeling the cyclic nature of these algorithms)
- send a message to some process (including itself)

Network latency can be modeled as a constant or based on a matrix of point-to-point latencies.
A sample matrix, based on latencies observed from PlanetLab, is provided for convenience.
In both cases it is possible to specify a drift for the given latency, modeling network uncertainty.

Process asynchrony is modeled by allowing processes to deviate arbitrarily from its scheduled execution.
Message processing is assumed to be instant though.
This can be overcome by having the application send the received message to itself at an arbitrary point in time.

Message loss is modeled by dropping messages uniformly at random, non uniform schemes could be easily implemented.
Churn is implemented following a similar mechanism.

All configurations are specified in a conf.yaml file.


## Running

Packages required
- difflib
- yaml
- cPickle

How to run:
- Use PyPy (tested with 1.9.0), otherwise simulations take too long
- Configuration is described in an yaml file called conf.yaml, 
  see conf_echo/conf.yaml for details
- Invocation pypy protocol.py <conf_directory> <run_number>
- This is CPU and RAM bound, if you have enough cores and RAM it's better
  to run several processes in parallel, for instance:
```
 $ for i in {1..10}; do
 $ time $pypy echo.py conf_echo/ $i > conf_echo/run-$i.log $
 $ done
```
- Then you can compute the required stats for all runs just by iterating over the produced results.
  utils.py provides some common statistical functions and other utils such as dumping to GnuPlot format.


## License
Copyright (C) 2018 Miguel Matos

This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.

You should have received a copy of the GNU General Public License along with this program. If not, see https://www.gnu.org/licenses/.
