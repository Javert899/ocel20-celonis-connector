# ocel20-celonis-connector

## Introduction

Celonis has established itself as the foremost process mining vendor in recent times, pioneering a variety of features that cater to the intricacies of the evolving business process landscape. One of the significant advances introduced by Celonis is its support for object-centric process mining via its "process sphere" feature. This capability illuminates the end-to-end process, accommodating various object types and showcasing a comprehensive view of business activities.

The importance of this approach is underpinned by the Celonis object-centric data model, which forms the bedrock for the "process sphere" feature. In light of this, we present our connector, designed specifically to bridge the gap between the world of object-centric event logs and the Celonis object-centric data model.

1. **Downloading Celonis Object-centric Data Model**: Using `celonis_download.py`, users can effortlessly download a Celonis object-centric data model, converting it into an object-centric event log in the OCEL 2.0 format. This ensures data compatibility and eases the transition for analysts and researchers familiar with the OCEL 2.0 format.

2. **Uploading OCEL 2.0 Event Logs**: The `new_celonis_ocel_upload.py` script facilitates the upload of OCEL 2.0 event logs directly to the Celonis object-centric data model. This is pivotal for businesses and analysts who maintain or analyze their logs in the OCEL 2.0 format and wish to leverage the superior analytical capabilities of the Celonis platform.

## Requirements

**pm4py** version >= 2.7.6 and **Pycelonis** .
