nudb\_use.variables.derive package
==================================

Derive several variables by column name::

    from nudb_use.variables import derive

    df = df.pipe(
        derive.derive_variables,
        ["utd_erutland", "utd_skoleaar_slutt"],
        priority="old",
    )

The functions run in the supplied order, deriving missing prerequisites through
the existing wrappers. Unknown names are logged and skipped; pass
``raise_errors=True`` to raise errors instead.

Derive the variables configured as relevant for a dataset with a dataframe pipe::

    from nudb_use.datasets import NudbData
    from nudb_use.variables import derive

    df = (
        NudbData("avslutta")
        .select("fnr", "utd_slutt")
        .df()
        .pipe(derive.pipe_all_relevant_variables_dataset, "avslutta")
    )

The helper selects variables whose ``derived_relevant_datasets`` contains the
dataset name, then applies their derive functions in configuration order.
Existing derive wrappers handle prerequisites. Variables without a registered
derive function are logged and skipped.

.. autofunction:: nudb_use.variables.derive.pipe_all_relevant_variables_dataset

To inspect the configured variable names before deriving them::

    variable_names = derive.get_relevant_variable_names_dataset("avslutta")

Names are returned in configuration order, including variables without a
registered derive function.

.. autofunction:: nudb_use.variables.derive.get_relevant_variable_names_dataset


nudb\_use.variables.derive.all\_data\_helpers module
----------------------------------------------------

.. automodule:: nudb_use.variables.derive.all_data_helpers
   :members:
   :show-inheritance:
   :undoc-members:

nudb\_use.variables.derive.bof module
-------------------------------------

.. automodule:: nudb_use.variables.derive.bof
   :members:
   :show-inheritance:
   :undoc-members:

nudb\_use.variables.derive.derive\_decorator module
---------------------------------------------------

.. automodule:: nudb_use.variables.derive.derive_decorator
   :members:
   :show-inheritance:
   :undoc-members:

nudb\_use.variables.derive.derive\_decorator\_utils module
----------------------------------------------------------

.. automodule:: nudb_use.variables.derive.derive_decorator_utils
   :members:
   :show-inheritance:
   :undoc-members:

nudb\_use.variables.derive.fullfoert module
-------------------------------------------

.. automodule:: nudb_use.variables.derive.fullfoert
   :members:
   :show-inheritance:
   :undoc-members:

nudb\_use.variables.derive.fullfoert\_foerste module
----------------------------------------------------

.. automodule:: nudb_use.variables.derive.fullfoert_foerste
   :members:
   :show-inheritance:
   :undoc-members:

nudb\_use.variables.derive.klass\_correspondences\_and\_variants module
-----------------------------------------------------------------------

.. automodule:: nudb_use.variables.derive.klass_correspondences_and_variants
   :members:
   :show-inheritance:
   :undoc-members:

nudb\_use.variables.derive.klass\_labels module
-----------------------------------------------

.. automodule:: nudb_use.variables.derive.klass_labels
   :members:
   :show-inheritance:
   :undoc-members:

nudb\_use.variables.derive.land module
--------------------------------------

.. automodule:: nudb_use.variables.derive.land
   :members:
   :show-inheritance:
   :undoc-members:

nudb\_use.variables.derive.nus\_variants module
-----------------------------------------------

.. automodule:: nudb_use.variables.derive.nus_variants
   :members:
   :show-inheritance:
   :undoc-members:

nudb\_use.variables.derive.person module
----------------------------------------

.. automodule:: nudb_use.variables.derive.person
   :members:
   :show-inheritance:
   :undoc-members:

nudb\_use.variables.derive.person\_idents module
------------------------------------------------

.. automodule:: nudb_use.variables.derive.person_idents
   :members:
   :show-inheritance:
   :undoc-members:

nudb\_use.variables.derive.registrert module
--------------------------------------------

.. automodule:: nudb_use.variables.derive.registrert
   :members:
   :show-inheritance:
   :undoc-members:

nudb\_use.variables.derive.registrert\_foerste module
-----------------------------------------------------

.. automodule:: nudb_use.variables.derive.registrert_foerste
   :members:
   :show-inheritance:
   :undoc-members:

nudb\_use.variables.derive.uh\_univ\_eller\_hoegskole module
------------------------------------------------------------

.. automodule:: nudb_use.variables.derive.uh_univ_eller_hoegskole
   :members:
   :show-inheritance:
   :undoc-members:

nudb\_use.variables.derive.utd\_foreldres\_utdnivaa module
----------------------------------------------------------

.. automodule:: nudb_use.variables.derive.utd_foreldres_utdnivaa
   :members:
   :show-inheritance:
   :undoc-members:

nudb\_use.variables.derive.utd\_hoeyeste module
-----------------------------------------------

.. automodule:: nudb_use.variables.derive.utd_hoeyeste
   :members:
   :show-inheritance:
   :undoc-members:

nudb\_use.variables.derive.utd\_skoleaar module
-----------------------------------------------

.. automodule:: nudb_use.variables.derive.utd_skoleaar
   :members:
   :show-inheritance:
   :undoc-members:
