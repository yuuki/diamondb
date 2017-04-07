REGION=ap-northeast-1
STACK_NAME=diamondb
CFCMD=aws cloudformation --region $(REGION)
PARAMETERS ?= "file://./parameters.json"
TEMPLATE="file://$(PWD)/template.yaml"
TIMESTAMP=$(shell date +%Y%m%d%H%M%S)
CHANGESET_NAME="$(STACK_NAME)-change-set-${TIMESTAMP}"

.PHONY: create
create:
	$(CFCMD) create-stack --stack-name $(STACK_NAME) --parameters $(PARAMETERS) --template-body $(TEMPLATE)
	$(CFCMD) wait stack-create-complete --stack-name $(STACK_NAME)

.PHONY: update
update:
	$(CFCMD) update-stack --stack-name $(STACK_NAME) --parameters $(PARAMETERS) --template-body $(TEMPLATE)
	$(CFCMD) wait stack-update-complete --stack-name $(STACK_NAME)

.PHONY: diff
diff:
	$(CFCMD) create-change-set --stack-name $(STACK_NAME) --parameters $(PARAMETERS) --template-body $(TEMPLATE) --change-set-name $(CHANGESET_NAME)
	@echo 'sleep for waiting change set deploy: '
	@sleep 5
	@echo 'changes: '
	$(CFCMD) describe-change-set --stack-name $(STACK_NAME) --change-set-name $(CHANGESET_NAME) | jq '.Changes'

.PHONY: delete-changesets
delete-changesets:
	@CHANGESETS="$(shell $(CFCMD) list-change-sets --stack-name $(STACK_NAME) | jq -r '.[][].ChangeSetName')"; \
	for name in $$CHANGESETS; do \
		$(CFCMD) delete-change-set --stack-name $(STACK_NAME) --change-set-name "$$name"; \
		echo "deleted change-set $(STACK_NAME) $$name"; \
	done
