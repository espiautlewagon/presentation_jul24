#!/bin/sh

jupyter nbconvert presentation.ipynb --to slides --post serve --no-prompt --TagRemovePreprocessor.remove_input_tags=remove_input --TagRemovePreprocessor.remove_all_outputs_tags=remove_output
