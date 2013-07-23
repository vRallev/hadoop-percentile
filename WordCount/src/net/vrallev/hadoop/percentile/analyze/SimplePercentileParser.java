package net.vrallev.hadoop.percentile.analyze;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;
import org.apache.commons.math3.stat.descriptive.rank.Percentile;

import java.io.*;

/**
 * @author Ralf Wondratschek
 */
public class SimplePercentileParser {

    private int[] mCountDirection;
    private int mCountTotal;

    private String[] mPercentiles;

    public SimplePercentileParser(File simulationCount, File direction) {
        try {
            parseSimulationCount(simulationCount);
            parseDirection(direction);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void print(PrintStream out) {
        for (int i = 0; i < mPercentiles.length; i++) {
            StringBuilder quantil = new StringBuilder("Q_").append((i + 1) / (double) mPercentiles.length);
            while(quantil.length() < "Q_0.00".length()) {
                quantil.append('0');
            }
            out.print(quantil.append(" = ").append(mPercentiles[i]));
        }
    }

    private void parseSimulationCount(File simulationCount) throws IOException {
        mCountDirection = new int[8];
        LineIterator iterator = FileUtils.lineIterator(simulationCount);
        try {
            while (iterator.hasNext()) {
                String[] line = iterator.nextLine().split("\t");
                String key = line[0].split("_")[1];

                if ("count".equals(key)) {
                    mCountTotal = Integer.parseInt(line[1]);
                } else {
                    mCountDirection[Integer.parseInt(key) / 45] = Integer.parseInt(line[1]);
                }

            }
        } finally {
            LineIterator.closeQuietly(iterator);
        }
    }

    private void parseDirection(File file) throws IOException {
        mPercentiles = new String[100];

        String direction = file.getName().substring(0, file.getName().lastIndexOf("."));
        int simulationCount = mCountDirection[Integer.parseInt(direction)];
        long chunkSize = getChunkSize(file, simulationCount);
        double stepSize = simulationCount / (double) mPercentiles.length;
        double skip = chunkSize * stepSize - chunkSize;

        BufferedInputStream bis = null;
        byte[] buffer = new byte[(int) chunkSize];

        try {
            bis = new BufferedInputStream(new FileInputStream(file));

            for (int i = 0; i < mPercentiles.length; i++) {
                while (skip < 0 && Math.abs(skip) >= chunkSize) {
                    // this check is used, when we have less simulations than percentiles
                    mPercentiles[i] = mPercentiles[i - 1];
                    skip += chunkSize * stepSize;
                    i++;
                }
                if (i >= mPercentiles.length) {
                    // check here again, because we incremented i
                    break;
                }

                while (skip >= chunkSize) {
                    skip(bis, chunkSize);
                    skip -= chunkSize;
                }

                // minus chunkSize, because we read chunkSize many bytes now
                skip += chunkSize * stepSize - chunkSize;

                read(bis, buffer);
                mPercentiles[i] = new String(buffer);
            }

        } finally {
            IOUtils.closeQuietly(bis);
        }
    }

    private static long getChunkSize(File file, int simulationCount) {
        return FileUtils.sizeOf(file) / simulationCount;
    }

    private static void skip(InputStream stream, long width) throws IOException {
        while (width > 0) {
            width -= stream.skip(width);
        }
    }

    private static void read(InputStream stream, byte[] buffer) throws IOException {
        int toRead = buffer.length;
        while (toRead > 0) {
            toRead -= stream.read(buffer, buffer.length - toRead, toRead);
        }
    }
}
