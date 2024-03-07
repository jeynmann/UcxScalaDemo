
# Copmile

``` sh
mvn -DskipTests clean package -Pdefault -T2C
```

# Run

## Demo

``` sh
LD_LIBRARY_PATH=openucx/ucx/install/lib scala -cp target/ucx-0.1-for-demo-jar-with-dependencies.jar  jeyn.demo.ucx.example.Demo -b 3337
LD_LIBRARY_PATH=openucx/ucx/install/lib scala -cp target/ucx-0.1-for-demo-jar-with-dependencies.jar  jeyn.demo.ucx.example.Demo -a 1.1.60.14:3337
```