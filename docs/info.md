```
{
  "inputs": [
    {
      "name": "samples",
      "label": "Samples",
      "type": "sample",
      "multiple": true,
      "required": true,
      "resolver": {
        "required_roles": [
          "FASTQ_R1",
          "FASTQ_R2"
        ]
      }
    },
    {
      "name": "reference",
      "label": "Reference Genome",
      "type": "file",
      "required": true,
      "resolver": {
        "accept_formats": [
          "fasta"
        ]
      }
    },
    {
      "name": "annotation",
      "label": "Annotation",
      "type": "file",
      "required": true,
      "resolver": {
        "accept_formats": [
          "gtf"
        ]
      }
    },
    {
      "name": "target",
      "label": "Target Region",
      "type": "file",
      "required": false,
      "resolver": {
        "accept_formats": [
          "bed"
        ]
      }
    }
  ]
}
```