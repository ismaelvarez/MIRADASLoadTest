# MIRADASLoadTest

Set arguments via command line

```console
loadTestDFAgent.py command --instrument-mode='SOL' --observation-type='OBJECTS' --observation-mode='always_success' --observation-class='SCIENCE' --number-petitions=100 --number-images=2 --petition-period=2 --image-path='/scidb/framedb/MIRADAS/2021-02-03/create_rect_transform/2021-02-03_15_47_18/remove_detector_signatures/2021-02-03_15_47_23/raw/'
```

Set arguments via config file

```console
loadTestDFAgent.py file --config-file DFAgentLoadTest.yaml
```
