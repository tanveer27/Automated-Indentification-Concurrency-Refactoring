# Automated-Indentification-Concurrency-Refactoring-
This repo contains the mining pipeline and Hybrid AST-LLM system desing for "Automated Identification and Evaluation of Concurrency Refactoring Opportunities in Java using Large Language Models"


Requirements:
For the Hybrid System:
- Python 3.x
- Ollama
- qwen2.5-coder:7b-instruct
- gson-2.10.1.jar
- javaparser-core-3.25.4.jar

For the mining Pipeline:
- Java 17+
- Maven
- Github API access token (set as environment variable GITHUB_TOKEN)

Running pipelines:


For Dataset A
1. First Run the Github Concurrency Miner this is will generate
a list of 100 repositories showing concurrecny opprotunites. The number can be changed in the code along with filter criteria.
2. Next run the GithubVerifyAndFirstUseFinder CSV. The CSV will take the ouput from step 1 as input as generate an output showing the first use of concurrency in the repos.
3. Finally run the GitHubIntoVSRefactorClassifier which will take the output from step 2 and mine for all the concurrency uses in the repos, exploring commits and identifying how they appeared in the project (as a first use or a refactor)


For Dataset B:
1. First Run the GithubNoConcurrencyMiner, this is will generate
a list of 10 (number can be changed) repositories that is mature and has no concurrency uses. The maturity filter may be tweaked inside the code.
2. Next run the RefactorOpportunitiesIdentifier which will take the output from step 1 and first verify all repos are concurrency free, then temporary download each repo to statically look for refactor opportunities using signatures provided in regex.


Running the the system:

For the Feasibility Study:
1. Put the candidate java files inside java folder on the root of the project.
2. Run the llm_generator.py file and the report will be generated in the test_out folder as a json file.