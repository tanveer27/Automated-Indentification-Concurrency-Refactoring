package scripts;

import com.github.javaparser.*;
import com.github.javaparser.ast.*;
import com.github.javaparser.ast.body.*;
import com.github.javaparser.ast.expr.MethodCallExpr;
import com.github.javaparser.ast.stmt.DoStmt; // <-- IMPORT ADDED
import com.github.javaparser.ast.stmt.ForEachStmt;
import com.github.javaparser.ast.stmt.ForStmt;
import com.github.javaparser.ast.stmt.WhileStmt;
import com.github.javaparser.ast.visitor.VoidVisitorAdapter;
import com.google.gson.*;

import java.io.File;
import java.io.IOException;
import java.util.*;

/**
    * This script parses a Java source file to extract candidate code blocks:
    * 1. Loops (for, foreach, while, do-while) - only the outermost loops are captured.
    * 2. Recursive methods (for ForkJoin)
    * The scope is limited to method level; class-level and global-level constructs are ignored.
    * Each candidate is enriched with contextual information including:
    * - Class name
    * - Method name
    * - Method signature
    * - Method parameters
    * - Local variables declared in the method
    * - Full method body (un-commented)
    * The output is a JSON object containing the package name, list of loops, and list of recursive methods.
    * Usage: java GenerateAST <JavaCode_FilePath>
 */
public class GenerateAST {

    public static void main(String[] args) throws IOException {
        if (args.length != 1) {
            System.err.println("Expected usage: java GenerateAST <JavaCode_FilePath>");
            System.exit(1);
        }

        CompilationUnit parsed;
        try {
             parsed = StaticJavaParser.parse(new File(args[0]));
        } catch (Exception e) {
            System.err.println("Failed to parse file: " + args[0]);
            e.printStackTrace(System.err);
            System.exit(1);
            return;
        }

        /**
         * Visit the AST to extract candidate loops and recursive methods
         */
        GenerateAST generator = new GenerateAST();
        CandidateExtractorVisitor visitor = generator.new CandidateExtractorVisitor();
        parsed.accept(visitor, null);

        // Prepare output JSON
        Map<String, Object> output = new LinkedHashMap<>();
        output.put("package", parsed.getPackageDeclaration().map(pd -> pd.getName().toString()).orElse(""));
        output.put("loops", visitor.getLoops());
        output.put("recursiveMethods", visitor.getRecursiveMethods());

        Gson gson = new GsonBuilder().disableHtmlEscaping().setPrettyPrinting().create();
        System.out.println(gson.toJson(output));
    }

    /**
     * This visitor finds all candidate code blocks:
     * 1. Loops (for, foreach, while, do-while) - *Only the outermost loop is captured.*
     * 2. Recursive methods (for ForkJoin)
     */
    private class CandidateExtractorVisitor extends VoidVisitorAdapter<Void> {
        private final List<Map<String, Object>> loops = new ArrayList<>();
        private final List<Map<String, Object>> recursiveMethods = new ArrayList<>();
        
        // This stack holds the context of the method we are currently inside
        private final Deque<Map<String, Object>> methodContextStack = new ArrayDeque<>();

        public List<Map<String, Object>> getLoops() {
            return loops;
        }
        
        public List<Map<String, Object>> getRecursiveMethods() {
            return recursiveMethods;
        }

        /**
         * When we enter a method, we build its "context" and check if it's recursive.
         */
        @Override
        public void visit(MethodDeclaration md, Void arg) {
            Map<String, Object> methodContext = new LinkedHashMap<>();
            methodContext.put("className", md.findAncestor(ClassOrInterfaceDeclaration.class).map(c -> c.getNameAsString()).orElse("UnknownClass"));
            methodContext.put("methodName", md.getNameAsString());
            methodContext.put("startLine", md.getBegin().map(p -> p.line).orElse(-1));
            methodContext.put("returnType", md.getType().asString());
            methodContext.put("methodSignature", md.getDeclarationAsString(false, false, false));

            // Get parameters
            List<Map<String, String>> params = new ArrayList<>();
            for (Parameter param : md.getParameters()) {
                Map<String, String> paramInfo = new HashMap<>();
                paramInfo.put("type", param.getType().asString());
                paramInfo.put("name", param.getNameAsString());
                params.add(paramInfo);
            }
            methodContext.put("parameters", params);

            // Get all local variables declared in this method
            List<Map<String, String>> methodLocalVariables = new ArrayList<>();
            md.findAll(VariableDeclarator.class).forEach(vd -> {
                Map<String, String> varInfo = new HashMap<>();
                varInfo.put("var_name", vd.getNameAsString());
                varInfo.put("var_type", vd.getType().asString());
                methodLocalVariables.add(varInfo);
            });
            methodContext.put("methodLocalVariables", methodLocalVariables);
            
            // This is the full, un-commented body of the method
            methodContext.put("fullMethodBody", md.getBody().map(Object::toString).orElse(""));

            // --- Check for Recursion ---
            boolean isRecursive = false;
            String methodName = md.getNameAsString();
            for (MethodCallExpr call : md.findAll(MethodCallExpr.class)) {
                if (call.getNameAsString().equals(methodName)) {

                    isRecursive = true;
                    break;
                }
            }

            if (isRecursive) {

                recursiveMethods.add(methodContext);
            }


            // Push this method's context onto the stack
            methodContextStack.push(methodContext);

            // Continue visiting children (which will find loops)
            super.visit(md, arg);

            // We are leaving this method, pop its context off the stack
            methodContextStack.pop();
        }

        // --- Loop Finders ---

        @Override
        public void visit(ForStmt n, Void arg) {
            if (!methodContextStack.isEmpty()) {
                loops.add(createLoopData("for", n.toString(), n, methodContextStack.peek()));
            }
            // DO NOT visit children (prevents nested loops)
        }

        @Override
        public void visit(ForEachStmt n, Void arg) {
            if (!methodContextStack.isEmpty()) {
                loops.add(createLoopData("foreach", n.toString(), n, methodContextStack.peek()));
            }
            // DO NOT visit children
        }

        @Override
        public void visit(WhileStmt n, Void arg) {
            if (!methodContextStack.isEmpty()) {
                loops.add(createLoopData("while", n.toString(), n, methodContextStack.peek()));
            }
            // DO NOT visit children
        }

        @Override
        public void visit(DoStmt n, Void arg) {
            if (!methodContextStack.isEmpty()) {
                loops.add(createLoopData("do-while", n.toString(), n, methodContextStack.peek()));
            }
            // DO NOT visit children
        }


        /**
         * Helper to package the loop info with its method context
         */
        private Map<String, Object> createLoopData(String type, String body, Node node, Map<String, Object> methodContext) {
            Map<String, Object> loopInfo = new LinkedHashMap<>();
            loopInfo.put("loopType", type);
            loopInfo.put("loopBody", body);
            loopInfo.put("startLine", node.getBegin().map(p -> p.line).orElse(-1));
            loopInfo.put("context", new LinkedHashMap<>(methodContext));
            return loopInfo;
        }
    }
}