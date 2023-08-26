package com.hzw.fdc.engine.calculatedindicator;


import javax.tools.*;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class InMemoryCompileEngine {

    public static boolean compileTest(String className, String sourceCode ) throws Throwable{
        JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
        DiagnosticCollector<JavaFileObject> diagnostics = new DiagnosticCollector<>();
        final JavaByteObject byteObject = new JavaByteObject(className);

        StandardJavaFileManager standardFileManager = compiler.getStandardFileManager(diagnostics, null, null);

        JavaFileManager fileManager = createFileManager(standardFileManager, byteObject);

        JavaCompiler.CompilationTask task = compiler.getTask(null, fileManager, diagnostics, null, null, getCompilationUnits(className,sourceCode));
        if (!task.call()) {
//            ByteArrayOutputStream os = new ByteArrayOutputStream();
//            PrintWriter printWriter = new PrintWriter(new OutputStreamWriter(os));
//            diagnostics.getDiagnostics().forEach(printWriter::println);
//            printWriter.close();
            List<Diagnostic<? extends JavaFileObject>> list = diagnostics.getDiagnostics().stream().collect(Collectors.toList());
            throw new CompileTestException(list);
        }
        fileManager.close();
        return true;
    }

    /**
     * 动态在内存中编译Java代码，返回指定的接口类型。
     * @param aClass        代码编译后所对应的java接口类型
     * @param className     源码所对应的类的SimpleName
     * @param sourceCode    源码字符串。 注意源码中的类名必需于className保持一致。
     * @param <T>
     * @return
     * @throws URISyntaxException
     * @throws ClassNotFoundException
     * @throws IllegalAccessException
     * @throws InstantiationException
     * @throws IOException
     */
    public  static <T> T loadClass(Class<T> aClass, String className, String sourceCode) throws URISyntaxException, ClassNotFoundException, IllegalAccessException, InstantiationException, IOException, CalculatedIndicatorBuildException {
        JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
        DiagnosticCollector<JavaFileObject> diagnostics = new DiagnosticCollector<>();

        final JavaByteObject byteObject = new JavaByteObject(className);

        StandardJavaFileManager standardFileManager = compiler.getStandardFileManager(diagnostics, null, null);

        JavaFileManager fileManager = createFileManager(standardFileManager, byteObject);

        JavaCompiler.CompilationTask task = compiler.getTask(null, fileManager, diagnostics, null, null, getCompilationUnits(className,sourceCode));

        if (!task.call()) {
            ByteArrayOutputStream os = new ByteArrayOutputStream();
            PrintWriter printWriter = new PrintWriter(new OutputStreamWriter(os));
            diagnostics.getDiagnostics().forEach(printWriter::println);
            printWriter.close();
            throw new CalculatedIndicatorBuildException(new String(os.toByteArray()));
        }
        fileManager.close();

        final ClassLoader inMemoryClassLoader = createClassLoader(byteObject);
        Class<T> instanceObjectClass = (Class<T>) inMemoryClassLoader.loadClass(className);
        T instanceObj = instanceObjectClass.newInstance();
        return instanceObj;
    }



    private static JavaFileManager createFileManager(StandardJavaFileManager fileManager,
                                                     JavaByteObject byteObject) {
        return new ForwardingJavaFileManager<StandardJavaFileManager>(fileManager) {
            @Override
            public JavaFileObject getJavaFileForOutput(Location location,
                                                       String className, JavaFileObject.Kind kind,
                                                       FileObject sibling) throws IOException {
                return byteObject;
            }
        };
    }

    private static ClassLoader createClassLoader(final JavaByteObject byteObject) {
        return new ClassLoader() {
            @Override
            public Class<?> findClass(String name) throws ClassNotFoundException {
                //这里不需要去搜索classpath，因为我们已经拿到了对象的字节码了。
                byte[] bytes = byteObject.getBytes();
                return defineClass(name, bytes, 0, bytes.length);
            }
        };
    }

    private static Iterable<? extends JavaFileObject> getCompilationUnits(String className, String sourceCode) {
        JavaStringObject stringObject = new JavaStringObject(className, sourceCode);
        return Arrays.asList(stringObject);
    }

}
