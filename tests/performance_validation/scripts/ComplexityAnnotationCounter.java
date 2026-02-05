/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * ComplexityAnnotationCounter - Automated validation script for complexity documentation.
 *
 * <p>This tool scans all Java source files in the Hadoop repository to identify methods
 * that meet algorithmic complexity threshold criteria and verifies the presence of
 * {@code @complexity} Javadoc annotations.</p>
 *
 * <h2>Threshold Criteria</h2>
 * <p>Methods meeting ANY of the following criteria require {@code @complexity} documentation:</p>
 * <ul>
 *   <li>Nested loops (2+ levels of for/while/do-while iteration)</li>
 *   <li>Recursive method calls (method calls itself or mutual recursion pattern)</li>
 *   <li>Collection processing with potential &gt;100 elements (List/Set/Map/array parameters)</li>
 *   <li>Memory allocation &gt;1KB patterns (new byte[1024+], StringBuilder with large capacity)</li>
 *   <li>Temporary collection creation (new ArrayList/HashMap in method body)</li>
 * </ul>
 *
 * <h2>Usage</h2>
 * <pre>
 * java ComplexityAnnotationCounter [source_directory]
 * 
 * Arguments:
 *   source_directory - Optional. Path to scan for Java files. 
 *                      Default: src/main/java
 * 
 * Exit Codes:
 *   0 - Pass: All threshold-meeting methods have @complexity annotations
 *   1 - Fail: One or more threshold-meeting methods lack @complexity annotations
 * </pre>
 *
 * <h2>Output</h2>
 * <ul>
 *   <li>Console: Summary statistics and violation list</li>
 *   <li>HTML Report: target/complexity-coverage-report.html</li>
 * </ul>
 *
 * @see <a href="https://hadoop.apache.org">Apache Hadoop</a>
 */
public class ComplexityAnnotationCounter {

    /** Default source directory to scan. */
    private static final String DEFAULT_SOURCE_DIR = "src/main/java";
    
    /** Output report path. */
    private static final String REPORT_OUTPUT_PATH = "target/complexity-coverage-report.html";
    
    /** Minimum allocation size in bytes to trigger threshold (1KB). */
    private static final int MIN_ALLOCATION_SIZE_BYTES = 1024;
    
    // -------------------------------------------------------------------------
    // Regex Patterns for Threshold Detection
    // -------------------------------------------------------------------------
    
    /**
     * Pattern to detect method declarations.
     * Matches: [modifiers] returnType methodName(params) [throws] {
     */
    private static final Pattern METHOD_DECLARATION_PATTERN = Pattern.compile(
        "^\\s*(?:(?:public|private|protected|static|final|synchronized|native|abstract|strictfp)\\s+)*" +
        "(?:<[^>]+>\\s+)?" +  // Optional generic type parameters
        "([\\w\\[\\]<>,\\s\\.]+)\\s+" +  // Return type (group 1)
        "(\\w+)\\s*" +  // Method name (group 2)
        "\\(([^)]*)\\)\\s*" +  // Parameters (group 3)
        "(?:throws\\s+[\\w\\s,\\.]+)?\\s*\\{",  // Optional throws clause
        Pattern.MULTILINE
    );
    
    /**
     * Pattern to detect nested for loops (2+ levels).
     * Looks for 'for' followed by content containing another 'for'.
     */
    private static final Pattern NESTED_FOR_LOOP_PATTERN = Pattern.compile(
        "for\\s*\\([^)]*\\)\\s*\\{[^}]*for\\s*\\([^)]*\\)",
        Pattern.DOTALL
    );
    
    /**
     * Pattern to detect nested while loops (2+ levels).
     */
    private static final Pattern NESTED_WHILE_LOOP_PATTERN = Pattern.compile(
        "while\\s*\\([^)]*\\)\\s*\\{[^}]*while\\s*\\([^)]*\\)",
        Pattern.DOTALL
    );
    
    /**
     * Pattern to detect for loop inside while loop or vice versa.
     */
    private static final Pattern MIXED_NESTED_LOOP_PATTERN = Pattern.compile(
        "(?:for|while)\\s*\\([^)]*\\)\\s*\\{[^}]*(?:for|while)\\s*\\([^)]*\\)",
        Pattern.DOTALL
    );
    
    /**
     * Pattern to detect collection parameters in method signature.
     * Matches List, Set, Map, Collection, Iterable, array types.
     */
    private static final Pattern COLLECTION_PARAM_PATTERN = Pattern.compile(
        "(?:List|Set|Map|Collection|Iterable|Queue|Deque)\\s*<|\\w+\\s*\\[\\s*\\]"
    );
    
    /**
     * Pattern to detect large byte array allocations (>= 1024 bytes).
     */
    private static final Pattern LARGE_BYTE_ALLOCATION_PATTERN = Pattern.compile(
        "new\\s+byte\\s*\\[\\s*(\\d+)\\s*\\]"
    );
    
    /**
     * Pattern to detect StringBuilder/StringBuffer with large initial capacity.
     */
    private static final Pattern LARGE_STRING_BUILDER_PATTERN = Pattern.compile(
        "new\\s+(?:StringBuilder|StringBuffer)\\s*\\(\\s*(\\d+)\\s*\\)"
    );
    
    /**
     * Pattern to detect temporary collection creation in method body.
     */
    private static final Pattern TEMP_COLLECTION_PATTERN = Pattern.compile(
        "new\\s+(?:ArrayList|LinkedList|HashSet|TreeSet|HashMap|TreeMap|" +
        "LinkedHashMap|LinkedHashSet|ConcurrentHashMap|CopyOnWriteArrayList|" +
        "ArrayDeque|PriorityQueue)\\s*[<(]"
    );
    
    /**
     * Pattern to detect @complexity annotation in Javadoc.
     */
    private static final Pattern COMPLEXITY_ANNOTATION_PATTERN = Pattern.compile(
        "@complexity\\s+",
        Pattern.CASE_INSENSITIVE
    );
    
    /**
     * Pattern to detect @PerformanceCritical inline comment marker.
     */
    private static final Pattern PERFORMANCE_CRITICAL_PATTERN = Pattern.compile(
        "//\\s*@PerformanceCritical"
    );
    
    /**
     * Pattern to detect Javadoc block preceding a method.
     */
    private static final Pattern JAVADOC_BLOCK_PATTERN = Pattern.compile(
        "/\\*\\*[^*]*\\*+(?:[^/*][^*]*\\*+)*/\\s*$",
        Pattern.MULTILINE | Pattern.DOTALL
    );

    // -------------------------------------------------------------------------
    // Inner Class: MethodInfo
    // -------------------------------------------------------------------------
    
    /**
     * Data class holding information about a detected threshold-meeting method.
     */
    public static class MethodInfo {
        private final String filePath;
        private final int lineNumber;
        private final String methodName;
        private final String methodSignature;
        private final List<String> thresholdReasons;
        private boolean hasComplexityAnnotation;
        private boolean hasPerformanceCriticalMarker;
        
        /**
         * Constructs a new MethodInfo instance.
         *
         * @param filePath path to the source file containing the method
         * @param lineNumber line number where the method is declared
         * @param methodName name of the method
         * @param methodSignature full method signature including parameters
         */
        public MethodInfo(String filePath, int lineNumber, String methodName, 
                          String methodSignature) {
            this.filePath = filePath;
            this.lineNumber = lineNumber;
            this.methodName = methodName;
            this.methodSignature = methodSignature;
            this.thresholdReasons = new ArrayList<>();
            this.hasComplexityAnnotation = false;
            this.hasPerformanceCriticalMarker = false;
        }
        
        public String getFilePath() {
            return filePath;
        }
        
        public int getLineNumber() {
            return lineNumber;
        }
        
        public String getMethodName() {
            return methodName;
        }
        
        public String getMethodSignature() {
            return methodSignature;
        }
        
        public List<String> getThresholdReasons() {
            return thresholdReasons;
        }
        
        public void addThresholdReason(String reason) {
            if (!thresholdReasons.contains(reason)) {
                thresholdReasons.add(reason);
            }
        }
        
        public boolean hasComplexityAnnotation() {
            return hasComplexityAnnotation;
        }
        
        public void setHasComplexityAnnotation(boolean hasAnnotation) {
            this.hasComplexityAnnotation = hasAnnotation;
        }
        
        public boolean hasPerformanceCriticalMarker() {
            return hasPerformanceCriticalMarker;
        }
        
        public void setHasPerformanceCriticalMarker(boolean hasMarker) {
            this.hasPerformanceCriticalMarker = hasMarker;
        }
        
        /**
         * Returns a formatted string representation for violation reporting.
         *
         * @return formatted violation string
         */
        public String toViolationString() {
            return String.format("VIOLATION: [%s:%d] %s - [%s]",
                filePath, lineNumber, methodName, 
                String.join(", ", thresholdReasons));
        }
        
        @Override
        public String toString() {
            return String.format("MethodInfo{file='%s', line=%d, method='%s', " +
                "reasons=%s, hasComplexity=%b, hasPerformanceCritical=%b}",
                filePath, lineNumber, methodName, thresholdReasons,
                hasComplexityAnnotation, hasPerformanceCriticalMarker);
        }
    }

    // -------------------------------------------------------------------------
    // Instance Variables
    // -------------------------------------------------------------------------
    
    /** List of all detected threshold-meeting methods. */
    private final List<MethodInfo> detectedMethods = new ArrayList<>();
    
    /** Count of files scanned. */
    private int filesScanned = 0;
    
    /** Count of files skipped due to errors. */
    private int filesSkipped = 0;
    
    /** Source directory being scanned. */
    private String sourceDirectory;

    // -------------------------------------------------------------------------
    // Main Entry Point
    // -------------------------------------------------------------------------
    
    /**
     * Main entry point for the ComplexityAnnotationCounter tool.
     *
     * <p>Accepts command-line arguments for source directory path.
     * Returns exit code 0 for pass (0 violations), 1 for fail (any violations).</p>
     *
     * @param args command-line arguments: [source_directory]
     */
    public static void main(String[] args) {
        String sourceDir = args.length > 0 ? args[0] : DEFAULT_SOURCE_DIR;
        
        System.out.println("============================================================");
        System.out.println("     ComplexityAnnotationCounter - Performance Doc Validator");
        System.out.println("============================================================");
        System.out.println();
        System.out.printf("Source Directory: %s%n", sourceDir);
        System.out.printf("Report Output: %s%n", REPORT_OUTPUT_PATH);
        System.out.println();
        
        ComplexityAnnotationCounter counter = new ComplexityAnnotationCounter();
        int exitCode = counter.run(sourceDir);
        
        System.exit(exitCode);
    }

    // -------------------------------------------------------------------------
    // Core Execution Methods
    // -------------------------------------------------------------------------
    
    /**
     * Executes the complexity annotation counting and validation process.
     *
     * @param sourceDir the source directory to scan
     * @return exit code: 0 for pass, 1 for fail
     */
    public int run(String sourceDir) {
        this.sourceDirectory = sourceDir;
        
        // Phase 1: Scan all Java files
        System.out.println("Phase 1: Scanning Java source files...");
        scanSourceDirectory(Paths.get(sourceDir));
        System.out.printf("  Scanned %d files, skipped %d files%n", 
            filesScanned, filesSkipped);
        System.out.println();
        
        // Phase 2: Generate report
        System.out.println("Phase 2: Generating coverage report...");
        generateHtmlReport();
        System.out.println("  Report generated: " + REPORT_OUTPUT_PATH);
        System.out.println();
        
        // Phase 3: Output summary
        return outputSummaryAndDetermineExitCode();
    }
    
    /**
     * Scans the source directory recursively for Java files.
     *
     * @param rootPath the root path to start scanning from
     */
    private void scanSourceDirectory(Path rootPath) {
        if (!Files.exists(rootPath)) {
            System.err.printf("ERROR: Source directory does not exist: %s%n", rootPath);
            return;
        }
        
        try (Stream<Path> pathStream = Files.walk(rootPath)) {
            pathStream
                .filter(Files::isRegularFile)
                .filter(this::isJavaSourceFile)
                .filter(this::shouldProcessFile)
                .forEach(this::processJavaFile);
        } catch (IOException e) {
            System.err.printf("ERROR: Failed to walk directory %s: %s%n", 
                rootPath, e.getMessage());
        }
    }
    
    /**
     * Checks if the given path is a Java source file.
     *
     * @param path the path to check
     * @return true if the file is a .java file
     */
    private boolean isJavaSourceFile(Path path) {
        return path.toString().endsWith(".java");
    }
    
    /**
     * Determines if a Java file should be processed.
     * Excludes test directories, package-info.java, and module-info.java files.
     *
     * @param path the path to check
     * @return true if the file should be processed
     */
    private boolean shouldProcessFile(Path path) {
        String pathStr = path.toString();
        String fileName = path.getFileName().toString();
        
        // Skip test directories
        if (pathStr.contains("/src/test/") || pathStr.contains("\\src\\test\\")) {
            return false;
        }
        
        // Skip package-info.java and module-info.java
        if (fileName.equals("package-info.java") || fileName.equals("module-info.java")) {
            return false;
        }
        
        // Skip generated source directories
        if (pathStr.contains("/generated/") || pathStr.contains("\\generated\\") ||
            pathStr.contains("/target/") || pathStr.contains("\\target\\")) {
            return false;
        }
        
        return true;
    }
    
    /**
     * Processes a single Java source file for threshold-meeting methods.
     *
     * @param filePath the path to the Java file
     */
    private void processJavaFile(Path filePath) {
        filesScanned++;
        
        try {
            String content = new String(Files.readAllBytes(filePath), StandardCharsets.UTF_8);
            String[] lines = content.split("\\r?\\n");
            
            // Find all method declarations and analyze them
            analyzeFileForMethods(filePath.toString(), content, lines);
            
        } catch (IOException e) {
            filesSkipped++;
            System.err.printf("WARNING: Failed to read file %s: %s%n", 
                filePath, e.getMessage());
        } catch (Exception e) {
            filesSkipped++;
            System.err.printf("WARNING: Error processing file %s: %s%n", 
                filePath, e.getMessage());
        }
    }
    
    /**
     * Analyzes a Java file content for methods meeting threshold criteria.
     *
     * @param filePath the file path string
     * @param content the full file content
     * @param lines the file content split into lines
     */
    private void analyzeFileForMethods(String filePath, String content, String[] lines) {
        // Find all method declarations
        List<MethodLocation> methods = findMethodDeclarations(content, lines);
        
        for (MethodLocation methodLoc : methods) {
            String methodBody = extractMethodBody(content, methodLoc.startIndex);
            if (methodBody == null || methodBody.isEmpty()) {
                continue;
            }
            
            // Check if method meets any threshold criteria
            MethodInfo methodInfo = analyzeMethodForThresholds(
                filePath, methodLoc, methodBody, content, lines);
            
            if (methodInfo != null && !methodInfo.getThresholdReasons().isEmpty()) {
                // Check for @complexity annotation
                checkForComplexityAnnotation(methodInfo, methodLoc, content);
                
                // Check for @PerformanceCritical marker
                checkForPerformanceCriticalMarker(methodInfo, methodBody);
                
                detectedMethods.add(methodInfo);
            }
        }
    }
    
    /**
     * Simple holder class for method location information.
     */
    private static class MethodLocation {
        final String methodName;
        final String methodSignature;
        final String parameters;
        final int startIndex;
        final int lineNumber;
        
        MethodLocation(String methodName, String methodSignature, String parameters,
                       int startIndex, int lineNumber) {
            this.methodName = methodName;
            this.methodSignature = methodSignature;
            this.parameters = parameters;
            this.startIndex = startIndex;
            this.lineNumber = lineNumber;
        }
    }
    
    /**
     * Finds all method declarations in the file content.
     *
     * @param content the file content
     * @param lines the file split into lines
     * @return list of method locations
     */
    private List<MethodLocation> findMethodDeclarations(String content, String[] lines) {
        List<MethodLocation> methods = new ArrayList<>();
        Matcher matcher = METHOD_DECLARATION_PATTERN.matcher(content);
        
        while (matcher.find()) {
            String returnType = matcher.group(1).trim();
            String methodName = matcher.group(2);
            String parameters = matcher.group(3);
            int startIndex = matcher.start();
            int lineNumber = getLineNumber(content, startIndex);
            
            // Skip constructors (return type same as containing class name) and
            // interface method signatures
            if (!isConstructorOrInterfaceMethod(content, startIndex, methodName)) {
                String signature = buildMethodSignature(returnType, methodName, parameters);
                methods.add(new MethodLocation(methodName, signature, parameters, 
                    startIndex, lineNumber));
            }
        }
        
        return methods;
    }
    
    /**
     * Determines if a method declaration is a constructor or interface method.
     *
     * @param content the file content
     * @param startIndex the start index of the method
     * @param methodName the method name
     * @return true if it's a constructor or interface method
     */
    private boolean isConstructorOrInterfaceMethod(String content, int startIndex, 
                                                    String methodName) {
        // Look back for class/interface declaration
        int searchStart = Math.max(0, startIndex - 500);
        String precedingContent = content.substring(searchStart, startIndex);
        
        // Check if inside interface (simplified check)
        if (precedingContent.contains("interface ") && 
            !precedingContent.contains("class ")) {
            return true;
        }
        
        // Check if it's a constructor by looking for class name match
        Pattern classPattern = Pattern.compile("class\\s+(\\w+)");
        Matcher classMatcher = classPattern.matcher(precedingContent);
        String lastClassName = null;
        while (classMatcher.find()) {
            lastClassName = classMatcher.group(1);
        }
        
        return methodName.equals(lastClassName);
    }
    
    /**
     * Builds a formatted method signature string.
     *
     * @param returnType the return type
     * @param methodName the method name
     * @param parameters the parameter list
     * @return formatted signature string
     */
    private String buildMethodSignature(String returnType, String methodName, 
                                         String parameters) {
        return String.format("%s %s(%s)", returnType, methodName, parameters);
    }
    
    /**
     * Gets the line number for a given character index in the content.
     *
     * @param content the full content
     * @param charIndex the character index
     * @return the line number (1-based)
     */
    private int getLineNumber(String content, int charIndex) {
        int lineNum = 1;
        for (int i = 0; i < charIndex && i < content.length(); i++) {
            if (content.charAt(i) == '\n') {
                lineNum++;
            }
        }
        return lineNum;
    }
    
    /**
     * Extracts the method body from the content starting at the given index.
     *
     * @param content the full file content
     * @param methodStartIndex the start index of the method declaration
     * @return the method body content, or null if extraction fails
     */
    private String extractMethodBody(String content, int methodStartIndex) {
        int braceIndex = content.indexOf('{', methodStartIndex);
        if (braceIndex == -1) {
            return null;
        }
        
        int braceCount = 1;
        int endIndex = braceIndex + 1;
        
        while (endIndex < content.length() && braceCount > 0) {
            char c = content.charAt(endIndex);
            if (c == '{') {
                braceCount++;
            } else if (c == '}') {
                braceCount--;
            }
            endIndex++;
        }
        
        if (braceCount != 0) {
            return null; // Unbalanced braces
        }
        
        return content.substring(braceIndex, endIndex);
    }
    
    /**
     * Analyzes a method for threshold criteria violations.
     *
     * @param filePath the file path
     * @param methodLoc the method location info
     * @param methodBody the method body content
     * @param content the full file content
     * @param lines the file split into lines
     * @return MethodInfo if thresholds are met, null otherwise
     */
    private MethodInfo analyzeMethodForThresholds(String filePath, MethodLocation methodLoc,
                                                   String methodBody, String content,
                                                   String[] lines) {
        MethodInfo info = new MethodInfo(filePath, methodLoc.lineNumber, 
            methodLoc.methodName, methodLoc.methodSignature);
        
        // Check for nested loops (2+ levels)
        if (hasNestedLoops(methodBody)) {
            info.addThresholdReason("Nested loops (2+ levels)");
        }
        
        // Check for recursive calls
        if (hasRecursiveCall(methodLoc.methodName, methodBody)) {
            info.addThresholdReason("Recursive method call");
        }
        
        // Check for collection parameters
        if (hasCollectionParameters(methodLoc.parameters)) {
            info.addThresholdReason("Collection parameter (potential >100 elements)");
        }
        
        // Check for large memory allocations
        if (hasLargeMemoryAllocation(methodBody)) {
            info.addThresholdReason("Large memory allocation (>1KB)");
        }
        
        // Check for temporary collection creation
        if (hasTemporaryCollectionCreation(methodBody)) {
            info.addThresholdReason("Temporary collection creation");
        }
        
        return info;
    }
    
    /**
     * Checks if the method body contains nested loops (2+ levels).
     *
     * @param methodBody the method body content
     * @return true if nested loops are detected
     */
    private boolean hasNestedLoops(String methodBody) {
        return NESTED_FOR_LOOP_PATTERN.matcher(methodBody).find() ||
               NESTED_WHILE_LOOP_PATTERN.matcher(methodBody).find() ||
               MIXED_NESTED_LOOP_PATTERN.matcher(methodBody).find();
    }
    
    /**
     * Checks if the method contains recursive calls to itself.
     *
     * @param methodName the method name
     * @param methodBody the method body content
     * @return true if recursive calls are detected
     */
    private boolean hasRecursiveCall(String methodName, String methodBody) {
        // Look for methodName followed by ( in the method body
        // This is a simplified check that may have false positives
        Pattern recursionPattern = Pattern.compile(
            "\\b" + Pattern.quote(methodName) + "\\s*\\(");
        return recursionPattern.matcher(methodBody).find();
    }
    
    /**
     * Checks if the method parameters include collection types.
     *
     * @param parameters the parameter list string
     * @return true if collection parameters are present
     */
    private boolean hasCollectionParameters(String parameters) {
        return COLLECTION_PARAM_PATTERN.matcher(parameters).find();
    }
    
    /**
     * Checks if the method body contains large memory allocations (>1KB).
     *
     * @param methodBody the method body content
     * @return true if large allocations are detected
     */
    private boolean hasLargeMemoryAllocation(String methodBody) {
        // Check byte array allocations
        Matcher byteArrayMatcher = LARGE_BYTE_ALLOCATION_PATTERN.matcher(methodBody);
        while (byteArrayMatcher.find()) {
            try {
                int size = Integer.parseInt(byteArrayMatcher.group(1));
                if (size >= MIN_ALLOCATION_SIZE_BYTES) {
                    return true;
                }
            } catch (NumberFormatException e) {
                // Skip invalid numbers
            }
        }
        
        // Check StringBuilder/StringBuffer with large initial capacity
        Matcher stringBuilderMatcher = LARGE_STRING_BUILDER_PATTERN.matcher(methodBody);
        while (stringBuilderMatcher.find()) {
            try {
                int size = Integer.parseInt(stringBuilderMatcher.group(1));
                // StringBuilder uses 2 bytes per char, so 512 chars = 1KB
                if (size >= MIN_ALLOCATION_SIZE_BYTES / 2) {
                    return true;
                }
            } catch (NumberFormatException e) {
                // Skip invalid numbers
            }
        }
        
        return false;
    }
    
    /**
     * Checks if the method body creates temporary collections.
     *
     * @param methodBody the method body content
     * @return true if temporary collections are created
     */
    private boolean hasTemporaryCollectionCreation(String methodBody) {
        return TEMP_COLLECTION_PATTERN.matcher(methodBody).find();
    }
    
    /**
     * Checks for @complexity annotation in the Javadoc preceding the method.
     *
     * @param methodInfo the method info to update
     * @param methodLoc the method location
     * @param content the full file content
     */
    private void checkForComplexityAnnotation(MethodInfo methodInfo, 
                                               MethodLocation methodLoc, 
                                               String content) {
        // Look for Javadoc block before the method
        int searchStart = Math.max(0, methodLoc.startIndex - 2000);
        String precedingContent = content.substring(searchStart, methodLoc.startIndex);
        
        // Find the last Javadoc block before the method
        int lastJavadocEnd = precedingContent.lastIndexOf("*/");
        if (lastJavadocEnd != -1) {
            int javadocStart = precedingContent.lastIndexOf("/**", lastJavadocEnd);
            if (javadocStart != -1) {
                String javadocContent = precedingContent.substring(javadocStart, 
                    lastJavadocEnd + 2);
                
                // Check for @complexity annotation
                if (COMPLEXITY_ANNOTATION_PATTERN.matcher(javadocContent).find()) {
                    methodInfo.setHasComplexityAnnotation(true);
                }
            }
        }
    }
    
    /**
     * Checks for @PerformanceCritical inline comment marker in the method.
     *
     * @param methodInfo the method info to update
     * @param methodBody the method body content
     */
    private void checkForPerformanceCriticalMarker(MethodInfo methodInfo, String methodBody) {
        if (PERFORMANCE_CRITICAL_PATTERN.matcher(methodBody).find()) {
            methodInfo.setHasPerformanceCriticalMarker(true);
        }
    }

    // -------------------------------------------------------------------------
    // Report Generation Methods
    // -------------------------------------------------------------------------
    
    /**
     * Generates the HTML coverage report.
     */
    private void generateHtmlReport() {
        // Ensure target directory exists
        Path targetDir = Paths.get("target");
        try {
            if (!Files.exists(targetDir)) {
                Files.createDirectories(targetDir);
            }
        } catch (IOException e) {
            System.err.printf("ERROR: Could not create target directory: %s%n", 
                e.getMessage());
            return;
        }
        
        try (BufferedWriter writer = new BufferedWriter(
                new FileWriter(REPORT_OUTPUT_PATH, StandardCharsets.UTF_8))) {
            
            int totalMethods = detectedMethods.size();
            int annotatedCount = (int) detectedMethods.stream()
                .filter(MethodInfo::hasComplexityAnnotation)
                .count();
            int missingCount = totalMethods - annotatedCount;
            double coveragePercent = totalMethods > 0 
                ? (annotatedCount * 100.0 / totalMethods) : 100.0;
            boolean isPassing = missingCount == 0;
            
            writer.write(generateHtmlContent(totalMethods, annotatedCount, 
                missingCount, coveragePercent, isPassing));
            
        } catch (IOException e) {
            System.err.printf("ERROR: Failed to write HTML report: %s%n", e.getMessage());
        }
    }
    
    /**
     * Generates the full HTML content for the report.
     *
     * @param totalMethods total number of threshold-meeting methods
     * @param annotatedCount number of methods with @complexity annotation
     * @param missingCount number of methods missing @complexity annotation
     * @param coveragePercent coverage percentage
     * @param isPassing whether the validation is passing (0 violations)
     * @return the complete HTML string
     */
    private String generateHtmlContent(int totalMethods, int annotatedCount,
                                        int missingCount, double coveragePercent,
                                        boolean isPassing) {
        StringBuilder html = new StringBuilder();
        String timestamp = LocalDateTime.now()
            .format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
        
        html.append("<!DOCTYPE html>\n");
        html.append("<html lang=\"en\">\n");
        html.append("<head>\n");
        html.append("    <meta charset=\"UTF-8\">\n");
        html.append("    <meta name=\"viewport\" content=\"width=device-width, initial-scale=1.0\">\n");
        html.append("    <title>Complexity Annotation Coverage Report</title>\n");
        html.append("    <style>\n");
        html.append(generateCssStyles());
        html.append("    </style>\n");
        html.append("</head>\n");
        html.append("<body>\n");
        html.append("    <div class=\"container\">\n");
        
        // Header
        html.append("        <h1>Complexity Annotation Coverage Report</h1>\n");
        html.append(String.format("        <p class=\"timestamp\">Generated: %s</p>\n", timestamp));
        html.append(String.format("        <p class=\"scan-info\">Source Directory: <code>%s</code></p>\n", 
            sourceDirectory));
        
        // Status Banner
        String statusClass = isPassing ? "status-pass" : "status-fail";
        String statusText = isPassing ? "PASS" : "FAIL";
        String statusMsg = isPassing 
            ? "All threshold-meeting methods have @complexity annotations!"
            : String.format("%d method(s) missing @complexity annotations", missingCount);
        
        html.append(String.format("        <div class=\"status-banner %s\">\n", statusClass));
        html.append(String.format("            <span class=\"status-text\">%s</span>\n", statusText));
        html.append(String.format("            <span class=\"status-message\">%s</span>\n", statusMsg));
        html.append("        </div>\n");
        
        // Summary Statistics
        html.append("        <div class=\"summary-section\">\n");
        html.append("            <h2>Summary Statistics</h2>\n");
        html.append("            <table class=\"summary-table\">\n");
        html.append("                <tr><th>Metric</th><th>Value</th></tr>\n");
        html.append(String.format("                <tr><td>Files Scanned</td><td>%d</td></tr>\n", 
            filesScanned));
        html.append(String.format("                <tr><td>Files Skipped</td><td>%d</td></tr>\n", 
            filesSkipped));
        html.append(String.format("                <tr><td>Threshold-Meeting Methods</td><td>%d</td></tr>\n", 
            totalMethods));
        html.append(String.format("                <tr><td>Annotated Methods</td><td>%d</td></tr>\n", 
            annotatedCount));
        html.append(String.format("                <tr><td>Missing Annotations</td><td class=\"%s\">%d</td></tr>\n", 
            missingCount > 0 ? "highlight-fail" : "", missingCount));
        html.append(String.format("                <tr><td>Coverage Percentage</td><td>%.2f%%</td></tr>\n", 
            coveragePercent));
        html.append("            </table>\n");
        html.append("        </div>\n");
        
        // Violations Section (if any)
        if (missingCount > 0) {
            html.append("        <div class=\"violations-section\">\n");
            html.append("            <h2>Methods Missing @complexity Annotation (Violations)</h2>\n");
            html.append("            <table class=\"violations-table\">\n");
            html.append("                <tr><th>File</th><th>Line</th><th>Method</th><th>Threshold Reasons</th></tr>\n");
            
            for (MethodInfo method : detectedMethods) {
                if (!method.hasComplexityAnnotation()) {
                    html.append(String.format(
                        "                <tr><td>%s</td><td>%d</td><td>%s</td><td>%s</td></tr>\n",
                        escapeHtml(method.getFilePath()),
                        method.getLineNumber(),
                        escapeHtml(method.getMethodName()),
                        escapeHtml(String.join(", ", method.getThresholdReasons()))
                    ));
                }
            }
            
            html.append("            </table>\n");
            html.append("        </div>\n");
        }
        
        // All Methods Section
        html.append("        <div class=\"all-methods-section\">\n");
        html.append("            <h2>All Threshold-Meeting Methods</h2>\n");
        html.append("            <table class=\"all-methods-table\">\n");
        html.append("                <tr><th>File</th><th>Line</th><th>Method</th><th>Threshold Reasons</th><th>@complexity</th><th>@PerformanceCritical</th></tr>\n");
        
        for (MethodInfo method : detectedMethods) {
            String complexityStatus = method.hasComplexityAnnotation() 
                ? "<span class=\"yes\">✓</span>" : "<span class=\"no\">✗</span>";
            String perfCriticalStatus = method.hasPerformanceCriticalMarker() 
                ? "<span class=\"yes\">✓</span>" : "<span class=\"na\">-</span>";
            
            html.append(String.format(
                "                <tr><td>%s</td><td>%d</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>\n",
                escapeHtml(method.getFilePath()),
                method.getLineNumber(),
                escapeHtml(method.getMethodName()),
                escapeHtml(String.join(", ", method.getThresholdReasons())),
                complexityStatus,
                perfCriticalStatus
            ));
        }
        
        html.append("            </table>\n");
        html.append("        </div>\n");
        
        // Footer
        html.append("        <div class=\"footer\">\n");
        html.append("            <p>ComplexityAnnotationCounter - Apache Hadoop Performance Documentation Validation</p>\n");
        html.append("        </div>\n");
        
        html.append("    </div>\n");
        html.append("</body>\n");
        html.append("</html>\n");
        
        return html.toString();
    }
    
    /**
     * Generates CSS styles for the HTML report.
     *
     * @return CSS style string
     */
    private String generateCssStyles() {
        return 
            "        body {\n" +
            "            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, sans-serif;\n" +
            "            line-height: 1.6;\n" +
            "            color: #333;\n" +
            "            margin: 0;\n" +
            "            padding: 20px;\n" +
            "            background-color: #f5f5f5;\n" +
            "        }\n" +
            "        .container {\n" +
            "            max-width: 1400px;\n" +
            "            margin: 0 auto;\n" +
            "            background: white;\n" +
            "            padding: 30px;\n" +
            "            border-radius: 8px;\n" +
            "            box-shadow: 0 2px 10px rgba(0,0,0,0.1);\n" +
            "        }\n" +
            "        h1 {\n" +
            "            color: #2c3e50;\n" +
            "            border-bottom: 3px solid #3498db;\n" +
            "            padding-bottom: 15px;\n" +
            "        }\n" +
            "        h2 {\n" +
            "            color: #34495e;\n" +
            "            margin-top: 30px;\n" +
            "        }\n" +
            "        .timestamp {\n" +
            "            color: #7f8c8d;\n" +
            "            font-size: 0.9em;\n" +
            "        }\n" +
            "        .scan-info {\n" +
            "            color: #7f8c8d;\n" +
            "            font-size: 0.9em;\n" +
            "        }\n" +
            "        .scan-info code {\n" +
            "            background: #ecf0f1;\n" +
            "            padding: 2px 6px;\n" +
            "            border-radius: 4px;\n" +
            "        }\n" +
            "        .status-banner {\n" +
            "            padding: 20px;\n" +
            "            border-radius: 8px;\n" +
            "            margin: 20px 0;\n" +
            "            display: flex;\n" +
            "            align-items: center;\n" +
            "            gap: 15px;\n" +
            "        }\n" +
            "        .status-pass {\n" +
            "            background: linear-gradient(135deg, #27ae60, #2ecc71);\n" +
            "            color: white;\n" +
            "        }\n" +
            "        .status-fail {\n" +
            "            background: linear-gradient(135deg, #c0392b, #e74c3c);\n" +
            "            color: white;\n" +
            "        }\n" +
            "        .status-text {\n" +
            "            font-size: 1.5em;\n" +
            "            font-weight: bold;\n" +
            "        }\n" +
            "        .status-message {\n" +
            "            font-size: 1.1em;\n" +
            "        }\n" +
            "        table {\n" +
            "            width: 100%;\n" +
            "            border-collapse: collapse;\n" +
            "            margin: 15px 0;\n" +
            "        }\n" +
            "        th, td {\n" +
            "            padding: 12px;\n" +
            "            text-align: left;\n" +
            "            border-bottom: 1px solid #ddd;\n" +
            "        }\n" +
            "        th {\n" +
            "            background: #3498db;\n" +
            "            color: white;\n" +
            "            font-weight: 600;\n" +
            "        }\n" +
            "        tr:hover {\n" +
            "            background: #f8f9fa;\n" +
            "        }\n" +
            "        .summary-table {\n" +
            "            max-width: 500px;\n" +
            "        }\n" +
            "        .highlight-fail {\n" +
            "            color: #e74c3c;\n" +
            "            font-weight: bold;\n" +
            "        }\n" +
            "        .violations-table tr {\n" +
            "            background: #fdf2f2;\n" +
            "        }\n" +
            "        .violations-table tr:hover {\n" +
            "            background: #fde8e8;\n" +
            "        }\n" +
            "        .yes {\n" +
            "            color: #27ae60;\n" +
            "            font-weight: bold;\n" +
            "        }\n" +
            "        .no {\n" +
            "            color: #e74c3c;\n" +
            "            font-weight: bold;\n" +
            "        }\n" +
            "        .na {\n" +
            "            color: #95a5a6;\n" +
            "        }\n" +
            "        .footer {\n" +
            "            margin-top: 40px;\n" +
            "            padding-top: 20px;\n" +
            "            border-top: 1px solid #ddd;\n" +
            "            text-align: center;\n" +
            "            color: #7f8c8d;\n" +
            "            font-size: 0.9em;\n" +
            "        }\n";
    }
    
    /**
     * Escapes HTML special characters in a string.
     *
     * @param text the text to escape
     * @return HTML-escaped text
     */
    private String escapeHtml(String text) {
        if (text == null) {
            return "";
        }
        return text
            .replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
            .replace("\"", "&quot;")
            .replace("'", "&#39;");
    }

    // -------------------------------------------------------------------------
    // Summary Output Methods
    // -------------------------------------------------------------------------
    
    /**
     * Outputs the summary to console and determines the exit code.
     *
     * @return exit code: 0 for pass, 1 for fail
     */
    private int outputSummaryAndDetermineExitCode() {
        int totalMethods = detectedMethods.size();
        int annotatedCount = (int) detectedMethods.stream()
            .filter(MethodInfo::hasComplexityAnnotation)
            .count();
        int missingCount = totalMethods - annotatedCount;
        double coveragePercent = totalMethods > 0 
            ? (annotatedCount * 100.0 / totalMethods) : 100.0;
        
        System.out.println("============================================================");
        System.out.println("                      SUMMARY");
        System.out.println("============================================================");
        System.out.printf("Scanned %d files, found %d threshold-meeting methods, " +
            "%d annotated, %d missing (%.2f%% coverage)%n",
            filesScanned, totalMethods, annotatedCount, missingCount, coveragePercent);
        System.out.println();
        
        if (missingCount > 0) {
            System.out.println("VIOLATIONS:");
            System.out.println("------------------------------------------------------------");
            for (MethodInfo method : detectedMethods) {
                if (!method.hasComplexityAnnotation()) {
                    System.out.println(method.toViolationString());
                }
            }
            System.out.println("------------------------------------------------------------");
            System.out.println();
            System.out.println("STATUS: FAIL - Documentation completeness not achieved");
            return 1;
        } else {
            System.out.println("STATUS: PASS - All threshold-meeting methods are documented");
            return 0;
        }
    }
    
    // -------------------------------------------------------------------------
    // Utility Methods for Testing
    // -------------------------------------------------------------------------
    
    /**
     * Gets the list of detected methods.
     * Useful for unit testing.
     *
     * @return list of detected MethodInfo objects
     */
    public List<MethodInfo> getDetectedMethods() {
        return new ArrayList<>(detectedMethods);
    }
    
    /**
     * Gets the count of files scanned.
     *
     * @return number of files scanned
     */
    public int getFilesScanned() {
        return filesScanned;
    }
    
    /**
     * Gets the count of files skipped.
     *
     * @return number of files skipped
     */
    public int getFilesSkipped() {
        return filesSkipped;
    }
    
    /**
     * Resets the counter state for re-running scans.
     * Useful for unit testing.
     */
    public void reset() {
        detectedMethods.clear();
        filesScanned = 0;
        filesSkipped = 0;
        sourceDirectory = null;
    }
}
