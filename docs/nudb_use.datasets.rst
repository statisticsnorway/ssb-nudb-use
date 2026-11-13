nudb\_use.datasets package
==========================

Dataset provenance
------------------

``nudb_dataset_id`` records the origin of a row, followed by derived dataset
names separated by ``>``. ``NudbData.from_parquet()`` adds the registered dataset
name to existing provenance, or starts a new value when the file has no such
column. External source loaders also supply the column.

``utd_hoeyeste`` retains the provenance of the selected education event.
``utd_foreldres_utdnivaa`` retains the winning parent's education provenance,
using the same ranking and tie rules as the education value. When neither parent
has an eligible education record, its provenance is null. This describes the
selected education's origin rather than every input used by the view.

``bu_igang`` starts fresh with
``cross-row-aggregate-igang-avslutta-eksamen-utd_hoeyeste>bu_igang`` because its
rows combine multiple sources and years. ``nuskat`` starts with
``nus2000-from-klass>nuskat``. BOF views start with the source filename stem,
including its period and version, and append the derived view's name.

Microdata keeps its variable-specific output format. Provenance is available
as ``MicroData("nudb_dataset_id")``, exposing the original event values from
``igang``, ``avslutta`` and ``eksamen`` without appending microdata dataset names.
Legacy NUDB loaders do not inject provenance when their source files lack it.

Subpackages
-----------

.. toctree::
   :maxdepth: 4

   nudb_use.datasets.microdata_variables


nudb\_use.datasets.avslutta module
----------------------------------

.. automodule:: nudb_use.datasets.avslutta
   :members:
   :show-inheritance:
   :undoc-members:

nudb\_use.datasets.bof module
-----------------------------

.. automodule:: nudb_use.datasets.bof
   :members:
   :show-inheritance:
   :undoc-members:

nudb\_use.datasets.eksamen module
---------------------------------

.. automodule:: nudb_use.datasets.eksamen
   :members:
   :show-inheritance:
   :undoc-members:

nudb\_use.datasets.external module
----------------------------------

.. automodule:: nudb_use.datasets.external
   :members:
   :show-inheritance:
   :undoc-members:

nudb\_use.datasets.igang module
-------------------------------

.. automodule:: nudb_use.datasets.igang
   :members:
   :show-inheritance:
   :undoc-members:

nudb\_use.datasets.macros module
--------------------------------

.. automodule:: nudb_use.datasets.macros
   :members:
   :show-inheritance:
   :undoc-members:

nudb\_use.datasets.microdata module
-----------------------------------

.. automodule:: nudb_use.datasets.microdata
   :members:
   :show-inheritance:
   :undoc-members:

nudb\_use.datasets.nudb\_data module
------------------------------------

.. automodule:: nudb_use.datasets.nudb_data
   :members:
   :show-inheritance:
   :undoc-members:

nudb\_use.datasets.nudb\_database module
----------------------------------------

.. automodule:: nudb_use.datasets.nudb_database
   :members:
   :show-inheritance:
   :undoc-members:

nudb\_use.datasets.nuskat module
--------------------------------

.. automodule:: nudb_use.datasets.nuskat
   :members:
   :show-inheritance:
   :undoc-members:

nudb\_use.datasets.old\_nudb\_data module
-----------------------------------------

.. automodule:: nudb_use.datasets.old_nudb_data
   :members:
   :show-inheritance:
   :undoc-members:

nudb\_use.datasets.person module
--------------------------------

.. automodule:: nudb_use.datasets.person
   :members:
   :show-inheritance:
   :undoc-members:

nudb\_use.datasets.snrkat module
--------------------------------

.. automodule:: nudb_use.datasets.snrkat
   :members:
   :show-inheritance:
   :undoc-members:

nudb\_use.datasets.utd\_foreldres\_utdnivaa module
--------------------------------------------------

.. automodule:: nudb_use.datasets.utd_foreldres_utdnivaa
   :members:
   :show-inheritance:
   :undoc-members:

nudb\_use.datasets.utd\_hoeyeste module
---------------------------------------

.. automodule:: nudb_use.datasets.utd_hoeyeste
   :members:
   :show-inheritance:
   :undoc-members:

nudb\_use.datasets.utils module
-------------------------------

.. automodule:: nudb_use.datasets.utils
   :members:
   :show-inheritance:
   :undoc-members:
