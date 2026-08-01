# doc_type classifier exercise (TF-IDF 1-2gram + LinearSVC, 5-fold CV)

Corpus: 706 docs, classes {'plenary_minutes': 619, 'md_minutes': 87}.

Accuracy: **0.872 ± 0.086** (folds: 0.930, 0.908, 0.702, 0.894, 0.929)

Caveat: labels are the REGEX classifier's own output, so this measures learnability/consistency of the regex labels, not ground truth. A human-labeled golden set is the next step (same recipe as the works-type classifier, project_siting_extension_works_classifier_2026_07_26).
