# Simulator for EpTO.

Sample implementation for 
An Epidemic Total Order Algorithm for Large-Scale Distributed Systems. 
Proceedings of the 16th Annual Middleware Conference. 
2015

Check http://www.gsd.inesc-id.pt/~mm/papers/2015/middleware_epto.pdf for the full paper.

##Usage

Packages required
- difflib
- yaml
- cPickle

How to run:
- Use PyPy (tested with 1.9.0), otherwise simulations take too long
- Configuration is described in an yaml file called conf.yaml, 
  see conf_epto/conf.yaml for details
- Invocation pypy <conf_directory> <run_number>
- This is CPU and RAM bound, if you have enough cores and RAM it's better
  to run several processes in parallel, for instance:

```
 $ for i in {1..10}; do
 $ time $pypy epto.py conf_epto/ $i > conf_epto/run-$i.log $
 $ done
```
- And then to obtain stats for all runs:
```
$pypy genStats.py conf_epto 10
```
- This will ouput the stats to stdout and also generate dumps in gnuplot format.
 
## License
Copyright (C) 2018 Miguel Matos

This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.

You should have received a copy of the GNU General Public License along with this program. If not, see https://www.gnu.org/licenses/.
