package utils;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/** A utility class for reading text files as list of string. */
public final class FileUtil {
  private static final Logger LOGGER = Logger.getLogger(FileUtil.class.getName());

  private FileUtil() {
    // Private constructor to prevent instantiation
  }

  /**
   * Read all lines from a given text file.
   *
   * @param filename The path to the file
   * @return A list of lines in the file represented as string, or an empty list when file doesn't
   *     exist or cannot be read.
   */
  public static List<String> readLines(String filename) {
    Path path = Paths.get(filename);

    if (!Files.exists(path)) {
      LOGGER.warning("File does not exist: " + filename);
      return List.of();
    }

    try {
      return Files.readAllLines(path, StandardCharsets.UTF_8);
    } catch (IOException e) {
      LOGGER.log(Level.SEVERE, "Failed to read file: " + filename, e);
      return List.of();
    }
  }
}
