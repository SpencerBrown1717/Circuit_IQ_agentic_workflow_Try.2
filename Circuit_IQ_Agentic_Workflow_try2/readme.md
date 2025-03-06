                   +-------------------+
                   |    Boss Agent     |
                   +--------+----------+
                            |
                            | Distributes tasks via
                            | message passing
                            v
    +------+------+------+------+------+------+
    |      |      |      |      |      |      |
    v      v      v      v      v      v      v
    
+------+ +------+ +------+ +------+ +------+ +------+     +---------+
|Copper| |Copper| |Solder| |Solder| |Silk- | |Drill | --> |  Gerber |
| Top  | |Bottom| |Mask T| |Mask B| |screen| |Agent | --> |   ZIP   |
+------+ +------+ +------+ +------+ +------+ +------+     +---------+ 

